package bio.ferlab.datalake.spark3.public.normalized

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLP
import bio.ferlab.datalake.spark3.utils.Coalesce
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.xml.{Elem, Node, XML}

class OrphanetGeneSet()(implicit conf: Configuration) extends ETLP {
  override val mainDestination: DatasetConf = conf.getDataset("normalized_orphanet_gene_set")
  val orphanet_gene_association: DatasetConf = conf.getDataset("raw_orphanet_gene_association")
  val orphanet_disease_history: DatasetConf = conf.getDataset("raw_orphanet_disease_history")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._

    def loadXML: String => Elem = str =>
      XML.loadString(spark.read.text(str).collect().map(_.getString(0)).mkString("\n"))

    Map(
      orphanet_gene_association.id -> parseProduct6XML(
        loadXML(orphanet_gene_association.location)
      ).toDF,
      orphanet_disease_history.id -> parseProduct9XML(
        loadXML(orphanet_disease_history.location)
      ).toDF
    )

  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    data(orphanet_gene_association.id)
      .join(
        data(orphanet_disease_history.id).select(
          "orpha_code",
          "average_age_of_onset",
          "average_age_of_death",
          "type_of_inheritance"
        ),
        Seq("orpha_code"),
        "left"
      )
  }

  private def getIdFromSourceName: (Node, String) => Option[String] =
    (genes, name) =>
      (genes \ "ExternalReferenceList" \ "ExternalReference")
        .find(node => (node \ "Source").text == name)
        .map(_ \ "Reference")
        .map(_.text)

  val ensembl_gene_id: Node => Option[String] = genes => getIdFromSourceName(genes, "Ensembl")
  val genatlas_gene_id: Node => Option[String] = genes => getIdFromSourceName(genes, "Genatlas")
  val HGNC_gene_id: Node => Option[String] = genes => getIdFromSourceName(genes, "HGNC")
  val omim_gene_id: Node => Option[String] = genes => getIdFromSourceName(genes, "OMIM")
  val reactome_gene_id: Node => Option[String] = genes => getIdFromSourceName(genes, "Reactome")
  val swiss_prot_gene_id: Node => Option[String] = genes => getIdFromSourceName(genes, "SwissProt")

  val association_type: Node => Option[String] =
    geneAssociation =>
      (geneAssociation \ "DisorderGeneAssociationType" \ "Name").headOption.map(_.text)

  val association_type_id: Node => Option[Long] =
    geneAssociation =>
      (geneAssociation \ "DisorderGeneAssociationType").headOption
        .flatMap(_.attribute("id"))
        .map(_.text.toLong)

  val association_status: Node => Option[String] =
    geneAssociation =>
      (geneAssociation \ "DisorderGeneAssociationStatus" \ "Name").headOption.map(_.text)

  def parseProduct6XML(doc: Elem): Seq[OrphanetGeneAssociation] = {
    for {
      disorder <- doc \\ "DisorderList" \\ "Disorder"
      orphaNumber <- disorder \ "OrphaCode"
      expertLink <- disorder \ "ExpertLink"
      name <- disorder \ "Name"
      disorderType <- disorder \ "DisorderType"
      disorderTypeName <- disorderType \ "Name"
      disorderGroup <- disorder \ "DisorderGroup"
      disorderGroupName <- disorderGroup \ "Name"
      geneAssociation <- disorder \ "DisorderGeneAssociationList" \ "DisorderGeneAssociation"
      genes <- geneAssociation \ "Gene"
      locus <- genes \ "LocusList" \ "Locus"
    } yield {
      OrphanetGeneAssociation(
        disorder.attribute("id").get.text.toLong,
        orphaNumber.text.toLong,
        expertLink.text,
        name.text,
        disorderType.attribute("id").get.text.toLong,
        disorderTypeName.text,
        disorderGroup.attribute("id").get.text.toLong,
        disorderGroupName.text,
        (geneAssociation \ "SourceOfValidation").text,
        genes.attribute("id").get.text.toLong,
        (genes \ "Symbol").text,
        (genes \ "Name").text,
        (genes \ "SynonymList" \\ "Synonym").map(_.text).toList,
        ensembl_gene_id(genes),
        genatlas_gene_id(genes),
        HGNC_gene_id(genes),
        omim_gene_id(genes),
        reactome_gene_id(genes),
        swiss_prot_gene_id(genes),
        association_type(geneAssociation),
        association_type_id(geneAssociation),
        association_status(geneAssociation),
        locus.attribute("id").get.text.toLong,
        (genes \ "LocusList" \ "Locus" \ "GeneLocus").text,
        (genes \ "LocusList" \ "Locus" \ "LocusKey").text.toLong
      )
    }
  }

  def parseProduct9XML(doc: Elem): Seq[OrphanetDiseaseHistory] = {
    for {
      disorder <- doc \\ "DisorderList" \\ "Disorder"
      orphaCode <- disorder \ "OrphaCode"
      expertLink <- disorder \ "ExpertLink"
      name <- disorder \ "Name"
      disorderType <- disorder \ "DisorderType"
      disorderTypeName <- disorderType \ "Name"
      disorderGroup <- disorder \ "DisorderGroup"
      disorderGroupName <- disorderType \ "Name"
    } yield {
      OrphanetDiseaseHistory(
        disorder.attribute("id").get.text.toLong,
        orphaCode.text.toLong,
        expertLink.text,
        name.text,
        disorderType.attribute("id").get.text.toLong,
        disorderTypeName.text,
        disorderGroup.attribute("id").get.text.toLong,
        disorderGroupName.text,
        (disorder \\ "AverageAgeOfOnsetList" \ "AverageAgeOfOnset" \ "Name").map(_.text).toList,
        (disorder \\ "AverageAgeOfDeathList" \ "AverageAgeOfDeath" \ "Name").map(_.text).toList,
        (disorder \\ "TypeOfInheritanceList" \ "TypeOfInheritance" \ "Name").map(_.text).toList
      )
    }
  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}

