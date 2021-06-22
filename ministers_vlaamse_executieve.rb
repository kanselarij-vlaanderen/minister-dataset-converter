require "linkeddata"
require "bson"
require "pry-byebug"
require "csv"
require 'date'


###
# RDF Utils
###
ORG = RDF::Vocab::ORG
FOAF = RDF::Vocab::FOAF
SKOS = RDF::Vocab::SKOS
PROV = RDF::Vocab::PROV
MU = RDF::Vocabulary.new("http://mu.semte.ch/vocabularies/core/")
BESLUIT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/besluit#")
MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")
GENERIEK = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/generiek#")
PERSON = RDF::Vocabulary.new("http://www.w3.org/ns/person#")
PERSOON = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/persoon#")

BASE_URI = "http://themis.vlaanderen.be/id/%{resource}/%{id}"
BESTUURSEENHEID_URI = "http://themis.vlaanderen.be/id/concept/%{resource}/%{id}"

###
# I/O Configuration
###
class Configuration
  def initialize
    @output_dir = "data/output"

    @output_file = "vandenbrande.ttl"
  end

  def output_file_path()
    "#{@output_dir}/#{@output_file}"
  end

  def to_s
    puts "Configuration:"
    puts "-- Output dir: #{@output_dir}"
  end
end

###
# Data conversion
###
config = Configuration.new
public_graph = RDF::Graph.new
graph = RDF::Graph.new

puts "[STARTED] Starting generating Vlaamse Executieve data"
puts config.to_s
puts ""

print "[ONGOING] Generating new bestuurseenheden for Vlaamse Gemeenschap..."

bestuursorgaan_vlaamse_regering = RDF::URI("http://themis.vlaanderen.be/id/bestuursorgaan/7f2c82aa-75ac-40f8-a6c3-9fe539163025")
bestuursorgaan_vlaams_parlement = RDF::URI("http://themis.vlaanderen.be/id/bestuursorgaan/17caac00-34a2-4b84-b400-30823555c15e")

uuid = BSON::ObjectId.new.to_s
bestuurseenheid_gemeenschap = RDF::URI(BESTUURSEENHEID_URI % { resource: "bestuurseenheid-classificatie", id: uuid })
public_graph << RDF.Statement(bestuurseenheid_gemeenschap, RDF.type, SKOS.Concept)
public_graph << RDF.Statement(bestuurseenheid_gemeenschap, MU.uuid, uuid)
public_graph << RDF.Statement(bestuurseenheid_gemeenschap, SKOS.prefLabel, "Gemeenschap")

uuid = BSON::ObjectId.new.to_s
bestuurseenheid_vlaamse_gemeenschap = RDF::URI(BASE_URI % { resource: "bestuurseenheid", id: uuid })
public_graph << RDF.Statement(bestuurseenheid_vlaamse_gemeenschap, RDF.type, BESLUIT.Bestuurseenheid)
public_graph << RDF.Statement(bestuurseenheid_vlaamse_gemeenschap, MU.uuid, uuid)
public_graph << RDF.Statement(bestuurseenheid_vlaamse_gemeenschap, SKOS.prefLabel, "Vlaamse Gemeenschap")
public_graph << RDF.Statement(bestuurseenheid_vlaamse_gemeenschap, ORG.classification, bestuurseenheid_gemeenschap)

public_graph << RDF.Statement(bestuursorgaan_vlaamse_regering, BESLUIT.bestuurt, bestuurseenheid_vlaamse_gemeenschap)
public_graph << RDF.Statement(bestuursorgaan_vlaams_parlement, BESLUIT.bestuurt, bestuurseenheid_vlaamse_gemeenschap)



puts " done"

print "[ONGOING] Generating new bestuursfuncties..."
bestuursfuncties_concept_scheme = RDF::URI("http://data.vlaanderen.be/id/conceptscheme/BestuursfunctieCode")

uuid = BSON::ObjectId.new.to_s
bestuursfunctie_voorzitter = RDF::URI(BASE_URI % { resource: "bestuursfunctie", id: uuid })
public_graph << RDF.Statement(bestuursfunctie_voorzitter, RDF.type, ORG.Role)
public_graph << RDF.Statement(bestuursfunctie_voorzitter, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursfunctie_voorzitter, SKOS.prefLabel, "Voorzitter")
public_graph << RDF.Statement(bestuursfunctie_voorzitter, SKOS.inScheme, bestuursfuncties_concept_scheme)

uuid = BSON::ObjectId.new.to_s
bestuursfunctie_vicevoorzitter = RDF::URI(BASE_URI % { resource: "bestuursfunctie", id: uuid })
public_graph << RDF.Statement(bestuursfunctie_vicevoorzitter, RDF.type, ORG.Role)
public_graph << RDF.Statement(bestuursfunctie_vicevoorzitter, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursfunctie_vicevoorzitter, SKOS.prefLabel, "Vice-voorzitter")
public_graph << RDF.Statement(bestuursfunctie_vicevoorzitter, SKOS.inScheme, bestuursfuncties_concept_scheme)

uuid = BSON::ObjectId.new.to_s
bestuursfunctie_gemeenschapsminister = RDF::URI(BASE_URI % { resource: "bestuursfunctie", id: uuid })
public_graph << RDF.Statement(bestuursfunctie_gemeenschapsminister, RDF.type, ORG.Role)
public_graph << RDF.Statement(bestuursfunctie_gemeenschapsminister, MU.uuid, uuid)
public_graph << RDF.Statement(bestuursfunctie_gemeenschapsminister, SKOS.prefLabel, "Gemeenschapsminister")
public_graph << RDF.Statement(bestuursfunctie_gemeenschapsminister, SKOS.inScheme, bestuursfuncties_concept_scheme)

puts " done"

legislatuur_vandenbrande = RDF::URI("http://themis.vlaanderen.be/id/bestuursorgaan/5fed907ce6670526694a0499")

print "[ONGOING] Generating legislatuur mandaat Voorzitter..."
uuid = BSON::ObjectId.new.to_s
mandaat_voorzitter = RDF::URI(BASE_URI % { resource: "mandaat", id: uuid })
public_graph << RDF.Statement(mandaat_voorzitter, RDF.type, MANDAAT.Mandaat)
public_graph << RDF.Statement(mandaat_voorzitter, MU.uuid, uuid)
public_graph << RDF.Statement(mandaat_voorzitter, ORG.role, bestuursfunctie_voorzitter)
public_graph << RDF.Statement(legislatuur_vandenbrande, ORG.hasPost, mandaat_voorzitter)
puts " done"

print "[ONGOING] Generating legislatuur mandaat Vice-voorzitter..."
uuid = BSON::ObjectId.new.to_s
mandaat_vice_voorzitter = RDF::URI(BASE_URI % { resource: "mandaat", id: uuid })
public_graph << RDF.Statement(mandaat_vice_voorzitter, RDF.type, MANDAAT.Mandaat)
public_graph << RDF.Statement(mandaat_vice_voorzitter, MU.uuid, uuid)
public_graph << RDF.Statement(mandaat_vice_voorzitter, ORG.role, bestuursfunctie_vicevoorzitter)
public_graph << RDF.Statement(legislatuur_vandenbrande, ORG.hasPost, mandaat_vice_voorzitter)
puts " done"

print "[ONGOING] Generating legislatuur mandaat Gemeenschapsminister..."
uuid = BSON::ObjectId.new.to_s
mandaat_gemeenschapsminister = RDF::URI(BASE_URI % { resource: "mandaat", id: uuid })
public_graph << RDF.Statement(mandaat_gemeenschapsminister, RDF.type, MANDAAT.Mandaat)
public_graph << RDF.Statement(mandaat_gemeenschapsminister, MU.uuid, uuid)
public_graph << RDF.Statement(mandaat_gemeenschapsminister, ORG.role, bestuursfunctie_gemeenschapsminister)
public_graph << RDF.Statement(legislatuur_vandenbrande, ORG.hasPost, mandaat_gemeenschapsminister)
puts " done"

print "[ONGOING] Generating new bestuursorganen..."

startRegering = DateTime.strptime("30/01/1992", "%d/%m/%Y")
endRegering = DateTime.strptime("19/10/1992", "%d/%m/%Y")
uuid = BSON::ObjectId.new.to_s
creatie = RDF::URI(BASE_URI % { resource: "creatie", id: uuid })
public_graph << RDF.Statement(creatie, MU.uuid, uuid)
public_graph << RDF.Statement(creatie, RDF.type, PROV.Generation)
public_graph << RDF.Statement(creatie, PROV.atTime, startRegering)

uuid = BSON::ObjectId.new.to_s
opheffing = RDF::URI(BASE_URI % { resource: "opheffing", id: uuid })
public_graph << RDF.Statement(opheffing, MU.uuid, uuid)
public_graph << RDF.Statement(opheffing, RDF.type, PROV.Invalidation)
public_graph << RDF.Statement(opheffing, PROV.atTime, endRegering)

uuid = BSON::ObjectId.new.to_s
vandenbrandeII = RDF::URI(BASE_URI % { resource: "bestuursorgaan", id: uuid })
public_graph << RDF.Statement(vandenbrandeII, MU.uuid, uuid)
public_graph << RDF.Statement(vandenbrandeII, RDF.type, BESLUIT.Bestuursorgaan)
public_graph << RDF.Statement(vandenbrandeII, SKOS.prefLabel, "Van den Brande II")
public_graph << RDF.Statement(vandenbrandeII, PROV.qualifiedGeneration, creatie)
public_graph << RDF.Statement(vandenbrandeII, PROV.qualifiedInvalidation, opheffing)
public_graph << RDF.Statement(vandenbrandeII, GENERIEK.isTijdspecialisatieVan, legislatuur_vandenbrande)



startRegering = DateTime.strptime("20/10/1992", "%d/%m/%Y")
endRegering = DateTime.strptime("19/6/1995", "%d/%m/%Y")
uuid = BSON::ObjectId.new.to_s
creatie = RDF::URI(BASE_URI % { resource: "creatie", id: uuid })
public_graph << RDF.Statement(creatie, MU.uuid, uuid)
public_graph << RDF.Statement(creatie, RDF.type, PROV.Generation)
public_graph << RDF.Statement(creatie, PROV.atTime, startRegering)

uuid = BSON::ObjectId.new.to_s
opheffing = RDF::URI(BASE_URI % { resource: "opheffing", id: uuid })
public_graph << RDF.Statement(opheffing, MU.uuid, uuid)
public_graph << RDF.Statement(opheffing, RDF.type, PROV.Invalidation)
public_graph << RDF.Statement(opheffing, PROV.atTime, endRegering)

uuid = BSON::ObjectId.new.to_s
vandenbrandeIII = RDF::URI(BASE_URI % { resource: "bestuursorgaan", id: uuid })
public_graph << RDF.Statement(vandenbrandeIII, MU.uuid, uuid)
public_graph << RDF.Statement(vandenbrandeIII, RDF.type, BESLUIT.Bestuursorgaan)
public_graph << RDF.Statement(vandenbrandeIII, SKOS.prefLabel, "Van den Brande III")
public_graph << RDF.Statement(vandenbrandeIII, PROV.qualifiedGeneration, creatie)
public_graph << RDF.Statement(vandenbrandeIII, PROV.qualifiedInvalidation, opheffing)
public_graph << RDF.Statement(vandenbrandeIII, GENERIEK.isTijdspecialisatieVan, legislatuur_vandenbrande)

puts " done"


print "[ONGOING] Writing generated data to files..."
RDF::Writer.open(config.output_file_path()) { |writer| writer << public_graph }
puts " done"

puts "[COMPLETED] Minister dataset conversion finished."