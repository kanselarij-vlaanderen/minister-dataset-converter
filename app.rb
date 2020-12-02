require "linkeddata"
require "bson"
require "pry-byebug"
require "csv"
require 'date'


###
# RDF Utils
###
ORG = RDF::Vocab::ORG
SKOS = RDF::Vocab::SKOS
MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
GENERIEK = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/generiek#")

BASE_URI = "http://themis.vlaanderen.be/id/%{resource}/%{id}"

###
# I/O Configuration
###
class Configuration
  def initialize
    @input_dir = "data/input"
    @output_dir = "data/output"

    @input_file = "mandaten.csv"

    @output_file = "mandaten.ttl"
  end

  def input_file_path()
    "#{@input_dir}/#{@input_file}"
  end

  def output_file_path()
    "#{@output_dir}/#{@output_file}"
  end

  def to_s
    puts "Configuration:"
    puts "-- Input dir: #{@input_dir}"
    puts "-- Output dir: #{@output_dir}"
  end
end

###
# Data conversion
###
config = Configuration.new
public_graph = RDF::Graph.new
graph = RDF::Graph.new

puts "[STARTED] Starting minister-dataset-conversion"
puts config.to_s
puts ""

print "[ONGOING] Generating bestuursfuncties..."
bestuursfuncties_concept_scheme = RDF::URI("http://data.vlaanderen.be/id/conceptscheme/BestuursfunctieCode")

uuid = BSON::ObjectId.new.to_s
bestuursfunctie_minister_president = RDF::URI(BASE_URI % { resource: "bestuursfunctie", id: uuid })
public_graph << RDF.Statement(bestuursfunctie_minister_president, RDF.type, ORG.Role)
public_graph << RDF.Statement(bestuursfunctie_minister_president, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursfunctie_minister_president, SKOS.prefLabel, "Minister-president")
public_graph << RDF.Statement(bestuursfunctie_minister_president, SKOS.inScheme, bestuursfuncties_concept_scheme)

uuid = BSON::ObjectId.new.to_s
bestuursfunctie_viceminister_president = RDF::URI(BASE_URI % { resource: "bestuursfunctie", id: uuid })
public_graph << RDF.Statement(bestuursfunctie_viceminister_president, RDF.type, ORG.Role)
public_graph << RDF.Statement(bestuursfunctie_viceminister_president, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursfunctie_viceminister_president, SKOS.prefLabel, "Viceminister-president")
public_graph << RDF.Statement(bestuursfunctie_viceminister_president, SKOS.inScheme, bestuursfuncties_concept_scheme)

uuid = BSON::ObjectId.new.to_s
bestuursfunctie_minister = RDF::URI(BASE_URI % { resource: "bestuursfunctie", id: uuid })
public_graph << RDF.Statement(bestuursfunctie_minister, RDF.type, ORG.Role)
public_graph << RDF.Statement(bestuursfunctie_minister, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursfunctie_minister, SKOS.prefLabel, "Minister")
public_graph << RDF.Statement(bestuursfunctie_minister, SKOS.inScheme, bestuursfuncties_concept_scheme)

uuid = BSON::ObjectId.new.to_s
bestuursfunctie_volksvertegenwoordiger = RDF::URI(BASE_URI % { resource: "bestuursfunctie", id: uuid })
public_graph << RDF.Statement(bestuursfunctie_volksvertegenwoordiger, RDF.type, ORG.Role)
public_graph << RDF.Statement(bestuursfunctie_volksvertegenwoordiger, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursfunctie_volksvertegenwoordiger, SKOS.prefLabel, "Volksvertegenwoordiger")
public_graph << RDF.Statement(bestuursfunctie_volksvertegenwoordiger, SKOS.inScheme, bestuursfuncties_concept_scheme)
puts " done"


print "[ONGOING] Generating bestuursorganen..."

uuid = BSON::ObjectId.new.to_s
bestuurseenheid_vlaams_gewest = RDF::URI(BASE_URI % { resource: "bestuurseenheid", id: uuid })
public_graph << RDF.Statement(bestuurseenheid_vlaams_gewest, RDF.type, BESLUIT.Bestuurseenheid)
public_graph << RDF.Statement(bestuurseenheid_vlaams_gewest, MU.uuid, uuid)
public_graph << RDF.Statement(bestuurseenheid_vlaams_gewest, SKOS.prefLabel, "Vlaams Gewest")
public_graph << RDF.Statement(bestuurseenheid_vlaams_gewest, SKOS.inScheme, 
  RDF::URI("http://themis.vlaanderen.be/id/concept/bestuurseenheid-classificatie/ba6abed5-a5b7-482b-9c19-18d51a4a6e6f"))

uuid = BSON::ObjectId.new.to_s
bestuursorgaan_vlaamse_regering = RDF::URI(BASE_URI % { resource: "bestuursorgaan", id: uuid })
public_graph << RDF.Statement(bestuursorgaan_vlaamse_regering, RDF.type, BESLUIT.Bestuursorgaan)
public_graph << RDF.Statement(bestuursorgaan_vlaamse_regering, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursorgaan_vlaamse_regering, SKOS.prefLabel, "Vlaamse Regering")
public_graph << RDF.Statement(bestuursorgaan_vlaamse_regering, SKOS.inScheme, 
  RDF::URI("http://themis.vlaanderen.be/id/concept/bestuursorgaan-classificatie/9682bad6-9c85-4eeb-9ac1-bba21666469a"))
public_graph << RDF.Statement(bestuursorgaan_vlaamse_regering, BESLUIT.bestuurt, bestuurseenheid_vlaams_gewest)

uuid = BSON::ObjectId.new.to_s
bestuursorgaan_vlaams_parlement = RDF::URI(BASE_URI % { resource: "bestuursorgaan", id: uuid })
public_graph << RDF.Statement(bestuursorgaan_vlaams_parlement, RDF.type, BESLUIT.Bestuursorgaan)
public_graph << RDF.Statement(bestuursorgaan_vlaams_parlement, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursorgaan_vlaams_parlement, SKOS.prefLabel, "Vlaams Parlement")
public_graph << RDF.Statement(bestuursorgaan_vlaams_parlement, SKOS.inScheme, 
  RDF::URI("http://themis.vlaanderen.be/id/concept/bestuursorgaan-classificatie/14b3f492-7106-46c7-9704-f547beff18ca"))
public_graph << RDF.Statement(bestuursorgaan_vlaams_parlement, BESLUIT.bestuurt, bestuurseenheid_vlaams_gewest)

puts " done"

file_path = File.join(config.input_file_path)

if not File.file?(file_path)
  puts "WARNING: #{file_path} not found"
  return
end

$name = ''
CSV.foreach(file_path, headers: true, encoding: "utf-8") do |row|
  if row["Reg of man"] == 'periode'
    print "[ONGOING] Generating legislatuur..."
    uuid = BSON::ObjectId.new.to_s
    $legislatuur = RDF::URI(BASE_URI % { resource: "legislatuur", id: uuid })
    public_graph << RDF.Statement($legislatuur, RDF.type, BESLUIT.Bestuursorgaan)
    public_graph << RDF.Statement($legislatuur, MU.uuid, uuid)
    public_graph << RDF.Statement($legislatuur, BESLUIT.bestuurt, bestuursorgaan_vlaamse_regering)
    puts " done"

    print "[ONGOING] Generating rechtstreekse verkiezing "
    print Date.strptime(row["Verkiezing"], "%m/%d/%Y").to_s + " ..."
    uuid = BSON::ObjectId.new.to_s
    rechtstreekse_verkiezing = RDF::URI(BASE_URI % { resource: "rechtstreekseverkiezing", id: uuid })
    public_graph << RDF.Statement(rechtstreekse_verkiezing, RDF.type, MANDAAT.RechtstreekseVerkiezing)
    public_graph << RDF.Statement(rechtstreekse_verkiezing, MU.uuid, uuid)
    public_graph << RDF.Statement(rechtstreekse_verkiezing, MANDAAT.datum, Date.strptime(row["Verkiezing"], "%m/%d/%Y"))
    public_graph << RDF.Statement(rechtstreekse_verkiezing, MANDAAT.steltSamen, $legislatuur)
    puts " done"
  end

  if (row["Reg of man"] == 'regering' && row["Naam"] != $name) 
    $name = row["Naam"]
    print "[ONGOING] Generating regering " + $name
    uuid = BSON::ObjectId.new.to_s
    bestuursorgaan_in_periode = RDF::URI(BASE_URI % { resource: "bestuursorgaaninperiode", id: uuid })
    public_graph << RDF.Statement(bestuursorgaan_in_periode, MU.uuid, uuid)
    public_graph << RDF.Statement(bestuursorgaan_in_periode, RDF.type, BESLUIT.Bestuursorgaan)
    public_graph << RDF.Statement(bestuursorgaan_in_periode, GENERIEK.isTijdspecialisatieVan, $legislatuur)
    puts " done"
  end
end  

print "[ONGOING] Writing generated data to files..."
RDF::Writer.open(config.output_file_path()) { |writer| writer << public_graph }
puts " done"

puts "[COMPLETED] Minister dataset conversion finished."