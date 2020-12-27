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

bestuursorgaan_vlaamse_regering = RDF::URI("http://themis.vlaanderen.be/id/bestuursorgaan/7f2c82aa-75ac-40f8-a6c3-9fe539163025")

file_path = File.join(config.input_file_path)

if not File.file?(file_path)
  puts "WARNING: #{file_path} not found"
  return
end

name = ''
personen_uri_map = {}
beleidsdomeinen_uri_map = {}
uuid = BSON::ObjectId.new.to_s
beleidsdomein_concept_scheme = RDF::URI(BASE_URI % { resource: "concept-scheme", id: uuid })
public_graph << RDF.Statement(beleidsdomein_concept_scheme, RDF.type, SKOS.ConceptScheme)
public_graph << RDF.Statement(beleidsdomein_concept_scheme, MU.uuid, uuid)
public_graph << RDF.Statement(beleidsdomein_concept_scheme, SKOS.prefLabel, "Beleidsdomeinen concept scheme")

legislatuur = nil
startLegislatuur = nil
endLegislatuur = nil
mandaat_minister_president = nil
mandaat_viceminister_president = nil
mandaat_minister = nil
bestuursorgaan_in_periode = nil

firstRegeringOfLegislatuur = true

CSV.foreach(file_path, headers: true, encoding: "utf-8") do |row|
  if row["Reg of man"] == 'periode'
    firstRegeringOfLegislatuur = true
    if (legislatuur)
      print "[ONGOING] Setting period for previous legislatuur #{startLegislatuur.strftime("%d/%m/%Y")} - #{endLegislatuur.strftime("%d/%m/%Y")}..."
      public_graph << RDF.Statement(legislatuur, SKOS.prefLabel, "Vlaamse Regering #{startLegislatuur.strftime("%d/%m/%Y")} - #{endLegislatuur.strftime("%d/%m/%Y")}")

      uuid = BSON::ObjectId.new.to_s
      creatieLegislatuur = RDF::URI(BASE_URI % { resource: "creatie", id: uuid })
      public_graph << RDF.Statement(creatieLegislatuur, MU.uuid, uuid)
      public_graph << RDF.Statement(creatieLegislatuur, RDF.type, PROV.Generation)
      public_graph << RDF.Statement(creatieLegislatuur, PROV.atTime, startLegislatuur)

      uuid = BSON::ObjectId.new.to_s
      opheffingLegislatuur = RDF::URI(BASE_URI % { resource: "opheffing", id: uuid })
      public_graph << RDF.Statement(opheffingLegislatuur, MU.uuid, uuid)
      public_graph << RDF.Statement(opheffingLegislatuur, RDF.type, PROV.Invalidation)
      public_graph << RDF.Statement(opheffingLegislatuur, PROV.atTime, endLegislatuur)

      public_graph << RDF.Statement(legislatuur, PROV.qualifiedGeneration, creatieLegislatuur)
      public_graph << RDF.Statement(legislatuur, PROV.qualifiedInvalidation, opheffingLegislatuur)
      puts " done"

      print "[ONGOING] Setting end of previous regering #{$endRegering.strftime("%d/%m/%Y")}..."
      uuid = BSON::ObjectId.new.to_s
      opheffing = RDF::URI(BASE_URI % { resource: "opheffing", id: uuid })
      public_graph << RDF.Statement(opheffing, MU.uuid, uuid)
      public_graph << RDF.Statement(opheffing, RDF.type, PROV.Invalidation)
      public_graph << RDF.Statement(opheffing, PROV.atTime, $endRegering)

      public_graph << RDF.Statement(bestuursorgaan_in_periode, PROV.qualifiedInvalidation, opheffing)
      puts " done"
    end

    print "[ONGOING] Generating legislatuur..."
    uuid = BSON::ObjectId.new.to_s
    legislatuur = RDF::URI(BASE_URI % { resource: "bestuursorgaan", id: uuid })
    public_graph << RDF.Statement(legislatuur, RDF.type, BESLUIT.Bestuursorgaan)
    public_graph << RDF.Statement(legislatuur, MU.uuid, uuid)
    public_graph << RDF.Statement(legislatuur, GENERIEK.isTijdspecialisatieVan, bestuursorgaan_vlaamse_regering)
    puts " done"

    print "[ONGOING] Generating rechtstreekse verkiezing "
    print Date.strptime(row["Verkiezing"], "%m/%d/%Y").to_s + " ..."
    uuid = BSON::ObjectId.new.to_s
    rechtstreekse_verkiezing = RDF::URI(BASE_URI % { resource: "rechtstreekse-verkiezing", id: uuid })
    public_graph << RDF.Statement(rechtstreekse_verkiezing, RDF.type, MANDAAT.RechtstreekseVerkiezing)
    public_graph << RDF.Statement(rechtstreekse_verkiezing, MU.uuid, uuid)
    public_graph << RDF.Statement(rechtstreekse_verkiezing, MANDAAT.datum, Date.strptime(row["Verkiezing"], "%m/%d/%Y"))
    public_graph << RDF.Statement(rechtstreekse_verkiezing, MANDAAT.steltSamen, legislatuur)
    puts " done"

    print "[ONGOING] Generating legislatuur mandaat Minister-President..."
    uuid = BSON::ObjectId.new.to_s
    mandaat_minister_president = RDF::URI(BASE_URI % { resource: "mandaat", id: uuid })
    public_graph << RDF.Statement(mandaat_minister_president, RDF.type, MANDAAT.Mandaat)
    public_graph << RDF.Statement(mandaat_minister_president, MU.uuid, uuid)
    public_graph << RDF.Statement(mandaat_minister_president, ORG.role, bestuursfunctie_minister_president)
    public_graph << RDF.Statement(legislatuur, ORG.hasPost, mandaat_minister_president)
    puts " done"

    print "[ONGOING] Generating legislatuur mandaat Viceminister-President..."
    uuid = BSON::ObjectId.new.to_s
    mandaat_viceminister_president = RDF::URI(BASE_URI % { resource: "mandaat", id: uuid })
    public_graph << RDF.Statement(mandaat_viceminister_president, RDF.type, MANDAAT.Mandaat)
    public_graph << RDF.Statement(mandaat_viceminister_president, MU.uuid, uuid)
    public_graph << RDF.Statement(mandaat_viceminister_president, ORG.role, bestuursfunctie_viceminister_president)
    public_graph << RDF.Statement(legislatuur, ORG.hasPost, mandaat_viceminister_president)
    puts " done"

    print "[ONGOING] Generating legislatuur mandaat Vlaamse minister..."
    uuid = BSON::ObjectId.new.to_s
    mandaat_minister = RDF::URI(BASE_URI % { resource: "mandaat", id: uuid })
    public_graph << RDF.Statement(mandaat_minister, RDF.type, MANDAAT.Mandaat)
    public_graph << RDF.Statement(mandaat_minister, MU.uuid, uuid)
    public_graph << RDF.Statement(mandaat_minister, ORG.role, bestuursfunctie_minister)
    public_graph << RDF.Statement(legislatuur, ORG.hasPost, mandaat_minister)
    puts " done"
  end


  if (row["Reg of man"] == 'regering') 
    $startRegering = DateTime.strptime(row["SamenstellingVan"], "%m/%d/%Y")
    $endRegering = DateTime.strptime(row["SamenstellingTot"], "%m/%d/%Y") if (row["SamenstellingTot"])

    if (row["Naam"] != name)
      name = row["Naam"]
      print "[ONGOING] Generating regering #{name} #{$startRegering}"

      if (firstRegeringOfLegislatuur)
        startLegislatuur = $startRegering
        firstRegeringOfLegislatuur = false
      end
      
      uuid = BSON::ObjectId.new.to_s
      creatie = RDF::URI(BASE_URI % { resource: "creatie", id: uuid })
      public_graph << RDF.Statement(creatie, MU.uuid, uuid)
      public_graph << RDF.Statement(creatie, RDF.type, PROV.Generation)
      public_graph << RDF.Statement(creatie, PROV.atTime, $startRegering)
      
      uuid = BSON::ObjectId.new.to_s
      bestuursorgaan_in_periode = RDF::URI(BASE_URI % { resource: "bestuursorgaan", id: uuid })
      public_graph << RDF.Statement(bestuursorgaan_in_periode, MU.uuid, uuid)
      public_graph << RDF.Statement(bestuursorgaan_in_periode, RDF.type, BESLUIT.Bestuursorgaan)
      public_graph << RDF.Statement(bestuursorgaan_in_periode, SKOS.prefLabel, name)
      public_graph << RDF.Statement(bestuursorgaan_in_periode, PROV.qualifiedGeneration, creatie)
      public_graph << RDF.Statement(bestuursorgaan_in_periode, GENERIEK.isTijdspecialisatieVan, legislatuur)

      if (row["SamenstellingTot"])
        $endRegering = DateTime.strptime(row["SamenstellingTot"], "%m/%d/%Y")
        endLegislatuur = $endRegering
      end

      puts " done"
    else 
      $endRegering = DateTime.strptime(row["SamenstellingTot"], "%m/%d/%Y") if (row["SamenstellingTot"])
      endLegislatuur = $endRegering
    end


  end

  if (row["Reg of man"] == 'mandaat')
    persoonId = row["AchternaamMandaathouder"] + " " + row["VoornaamMandaathouder"]
    unless personen_uri_map[persoonId]
      print "[ONGOING] Generating persoon #{persoonId}"
      uuid = BSON::ObjectId.new.to_s
      persoon = RDF::URI(BASE_URI % { resource: "persoon", id: uuid })
      public_graph << RDF.Statement(persoon, MU.uuid, uuid)
      public_graph << RDF.Statement(persoon, RDF.type, PERSON.Person)
      public_graph << RDF.Statement(persoon, FOAF.familyName, row["AchternaamMandaathouder"])
      public_graph << RDF.Statement(persoon, PERSOON.gebruikteVoornaam, row["VoornaamMandaathouder"] )
      personen_uri_map[persoonId] = persoon
      puts " done"
    end

    print "[ONGOING] Generating mandataris #{persoonId} ..."
    uuid = BSON::ObjectId.new.to_s
    mandataris = RDF::URI(BASE_URI % { resource: "mandataris", id: uuid })
    public_graph << RDF.Statement(mandataris, RDF.type, MANDAAT.Mandataris)
    public_graph << RDF.Statement(mandataris, MU.uuid, uuid)
    public_graph << RDF.Statement(mandataris, ORG.holds, mandaat_minister)
    public_graph << RDF.Statement(mandataris, MANDAAT.rangorde, row["rang"].to_i)
    public_graph << RDF.Statement(mandataris, MANDAAT.start, $startRegering)
    public_graph << RDF.Statement(mandataris, MANDAAT.einde, $endRegering)
    public_graph << RDF.Statement(mandataris, MANDAAT.isBestuurlijkeAliasVan, personen_uri_map[persoonId])

    public_graph << RDF.Statement(bestuursorgaan_in_periode, PROV.hadMember, mandataris)
    puts " done"

    fullTitle = row["Titel"] 
    unless fullTitle.nil?
      puts "Generating titles " + fullTitle
      titleList = fullTitle.match /(?<= van\s)(?<titles>.*)/
      titles = (titleList[:titles].strip || titleList[:titles]).split(/\s*[,]\s* | \s*en\s*/)
      puts titles

      titles.each do |title|
        unless beleidsdomeinen_uri_map[title]
          print "[ONGOING] Generating beleidsdomein #{title}"
          uuid = BSON::ObjectId.new.to_s
          beleidsdomein = RDF::URI(BASE_URI % { resource: "beleidsdomein", id: uuid })
          public_graph << RDF.Statement(beleidsdomein, RDF.type, SKOS.Concept)
          public_graph << RDF.Statement(beleidsdomein, MU.uuid, uuid)
          public_graph << RDF.Statement(beleidsdomein, SKOS.prefLabel, title)
          public_graph << RDF.Statement(beleidsdomein, SKOS.inScheme, beleidsdomein_concept_scheme)
          beleidsdomeinen_uri_map[title] = beleidsdomein
          puts " done"
        end

        print "[ONGOING] Adding title #{title} for mandataris #{persoonId} ..."
        public_graph << RDF.Statement(mandataris, MANDAAT.beleidsdomein, beleidsdomeinen_uri_map[title])
        puts " done"
      end
    end

    unless row["MP/Vice"].nil?
      if (row["MP/Vice"].downcase.include? "vice")
        print "[ONGOING] Adding functie viceminister for mandataris #{persoonId} ..."
        uuid = BSON::ObjectId.new.to_s
        mandataris = RDF::URI(BASE_URI % { resource: "mandataris", id: uuid })
        public_graph << RDF.Statement(mandataris, RDF.type, MANDAAT.Mandataris)
        public_graph << RDF.Statement(mandataris, MU.uuid, uuid)
        public_graph << RDF.Statement(mandataris, ORG.holds, mandaat_viceminister_president)
        public_graph << RDF.Statement(mandataris, MANDAAT.rangorde, row["rang"].to_i)
        public_graph << RDF.Statement(mandataris, MANDAAT.start, $startRegering)
        public_graph << RDF.Statement(mandataris, MANDAAT.einde, $endRegering)
        public_graph << RDF.Statement(mandataris, MANDAAT.isBestuurlijkeAliasVan, personen_uri_map[persoonId])
    
        public_graph << RDF.Statement(bestuursorgaan_in_periode, PROV.hadMember, mandataris)
        puts " done"
      else 
        print "[ONGOING] Adding functie minister-president for mandataris #{persoonId} ..."
        uuid = BSON::ObjectId.new.to_s
        mandataris = RDF::URI(BASE_URI % { resource: "mandataris", id: uuid })
        public_graph << RDF.Statement(mandataris, RDF.type, MANDAAT.Mandataris)
        public_graph << RDF.Statement(mandataris, MU.uuid, uuid)
        public_graph << RDF.Statement(mandataris, ORG.holds, mandaat_minister_president)
        public_graph << RDF.Statement(mandataris, MANDAAT.rangorde, row["rang"].to_i)
        public_graph << RDF.Statement(mandataris, MANDAAT.start, $startRegering)
        public_graph << RDF.Statement(mandataris, MANDAAT.einde, $endRegering)
        public_graph << RDF.Statement(mandataris, MANDAAT.isBestuurlijkeAliasVan, personen_uri_map[persoonId])
    
        public_graph << RDF.Statement(bestuursorgaan_in_periode, PROV.hadMember, mandataris)
        puts " done"
      end
    end
  end
end  

if (legislatuur)
  print "[ONGOING] Setting period for previous legislatuur #{startLegislatuur.strftime("%d/%m/%Y")} - ... "
  public_graph << RDF.Statement(legislatuur, SKOS.prefLabel, "Vlaamse Regering #{startLegislatuur.strftime("%d/%m/%Y")} - ... ")

  uuid = BSON::ObjectId.new.to_s
  creatieLegislatuur = RDF::URI(BASE_URI % { resource: "creatie", id: uuid })
  public_graph << RDF.Statement(creatieLegislatuur, MU.uuid, uuid)
  public_graph << RDF.Statement(creatieLegislatuur, RDF.type, PROV.Generation)
  public_graph << RDF.Statement(creatieLegislatuur, PROV.atTime, startLegislatuur)

  public_graph << RDF.Statement(legislatuur, PROV.qualifiedGeneration, creatieLegislatuur)

  puts " done"
end

print "[ONGOING] Writing generated data to files..."
RDF::Writer.open(config.output_file_path()) { |writer| writer << public_graph }
puts " done"

puts "[COMPLETED] Minister dataset conversion finished."