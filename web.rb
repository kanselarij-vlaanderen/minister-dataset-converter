require "csv"
require "linkeddata"

###
# RDF Utils
###
ORG = RDF::Vocab::ORG
FOAF = RDF::Vocab::FOAF
DCT = RDF::Vocab::DC
PERSON = RDF::Vocabulary.new("http://www.w3.org/ns/person#")
PERSOON = RDF::Vocabulary.new("https://data.vlaanderen.be/ns/persoon#")
OWL = RDF::Vocabulary.new("http://www.w3.org/2002/07/owl#")
MANDAAT = RDF::Vocabulary.new("http://data.vlaanderen.be/ns/mandaat#")

BASE_URI = "http://themis.vlaanderen.be/id/%{resource}/%{id}"

###
# I/O Configuration
###
class Configuration
  def initialize
    @input_dir = "/data/input"
    @output_dir = "/data/output"

    @input_files = {
      personen: "kaleidos-mandataris-personen.csv",
      mandaten: "kaleidos-mandatarissen.csv"
    }

    @output_file = "personen.ttl"
  end

  def input_file_path(file)
    "#{@input_dir}/#{@input_files[file]}"
  end

  def output_file_path()
    "#{@output_dir}/#{@output_file}"
  end

  def to_s
    log.info "Configuration:"
    log.info "-- Input dir: #{@input_dir}"
    log.info "-- Output dir: #{@output_dir}"
  end
end

###
# Data conversion
###
config = Configuration.new
public_graph = RDF::Graph.new

log.info "[STARTED] Starting personen-conversion"
log.info config.to_s

corrections = {
  "ggeens" => "geens",
  "llenssens" => "lenssens",
  "durpe" => "durpé",
  "akkkermans" => "akkermans",
  "kelchertermans" => "kelchtermans",
  "gabriels" => "gabriëls",
  "detiege" => "detiège",
  "batselier" => "de batselier",
  "buchman" => "buchmann",
  "van den brande - minister-president" => "van den brande",
  "gabriels jaak" => "gabriëls",
  "schitlz" => "schiltz",
  "de batselier." => "de batselier",
  "l." => "waltniel",
  "diependaele" => "diependaele",
  "dewulf" => "de wulf",
  "a. b." => "van den brande",
  "leo peeters" => "peeters"
}


personen_input_file = config.input_file_path(:personen)

personsNotFound = []
personUriMap = {}
CSV.foreach(personen_input_file, headers: true, encoding: "utf-8").with_index(1) do |row, index|
  familyName = row["familyName"].downcase.strip
  firstName = row["firstName"]
  fullName = row["fullName"]
  personUri = row["persoon"]

  familyName = corrections[familyName] if corrections.key? familyName

  familyName = "van den brande" if (familyName.include? "van den brande" )
  familyName = "demeester-de meyer" if (familyName.include? "meester" )

  personResult = nil
  if (familyName == "peeters" || familyName == "van den bossche")
    log.info "[ONGOING] Finding person for /#{firstName}/#{familyName}/(#{fullName})/"

    fullName = "luc" if (familyName == "van den bossche" && firstName == nil)
    fullName = "leo" if (familyName == "peeters" && firstName == nil)

    personQuery =  " SELECT ?personUri ?name ?firstName WHERE {"
    personQuery += "   GRAPH <#{ENV['DEFAULT_GRAPH']}> {"
    personQuery += "     ?personUri a <#{PERSON.Person}> ;"
    personQuery += "     <#{FOAF.familyName}> ?name ;"
    personQuery += "     <#{PERSOON.gebruikteVoornaam}> ?firstName ."
    personQuery += "     FILTER (lcase(str(?name)) = #{familyName.sparql_escape})"
    personQuery += "   }"
    personQuery += " }"

    personResult = query personQuery
    personResult = personResult.filter { |result| fullName.downcase.include? result.firstName.value.downcase }

    if personResult.first
      log.info "OK"
      themisUri = personResult.first.personUri.value
      public_graph << RDF.Statement(RDF::URI(themisUri), OWL.sameAs, RDF::URI(personUri))
      personUriMap[:personUri] = themisUri
    else 
      log.info "NOT OK"
      personsNotFound << "#{index} /#{firstName}/#{familyName}/(#{fullName})/#{personUri}"
    end
  else 

    log.info "[ONGOING] Finding person for /#{firstName}/#{familyName}/(#{fullName})/"

    personQuery =  " SELECT ?personUri ?name ?firstName WHERE {"
    personQuery += "   GRAPH <#{ENV['DEFAULT_GRAPH']}> {"
    personQuery += "     ?personUri a <#{PERSON.Person}> ;"
    personQuery += "     <#{FOAF.familyName}> ?name ;"
    personQuery += "     <#{PERSOON.gebruikteVoornaam}> ?firstName ."
    personQuery += "     FILTER (lcase(str(?name)) = #{familyName.sparql_escape})"
    personQuery += "   }"
    personQuery += " }"

    personResult = query personQuery

    if personResult.first
      log.info "OK"
      themisUri = personResult.first.personUri.value
      public_graph << RDF.Statement(RDF::URI(themisUri), OWL.sameAs, RDF::URI(personUri))
      personUriMap[personUri] = themisUri
    else 
      log.info "NOT OK"
      personsNotFound << "#{index} /#{firstName}/#{familyName}/(#{fullName})/#{personUri}"
    end
  end
end  

mandaten_input_file = config.input_file_path(:mandaten)

CSV.foreach(mandaten_input_file, headers: true, encoding: "utf-8") do |row|
  mandatarisUri = row["mandataris"]
  personUri = row["persoon"]
  themisPersonUri = personUriMap[personUri]

  public_graph << RDF.Statement(RDF::URI(themisPersonUri), DCT.relation, RDF::URI(mandatarisUri))
  public_graph << RDF.Statement(RDF::URI(mandatarisUri), RDF.type, MANDAAT.Mandaat)

end


log.info "Number of persons not found: #{personsNotFound.length()}"
personsNotFound.each do |person|
  log.info person
end

personUriMap.each do |entry|
  log.info entry
end

log.info "[ONGOING] Writing generated data to files... #{config.output_file_path()}"
RDF::Writer.open(config.output_file_path()) { |writer| writer << public_graph }
log.info " done"

log.info "[COMPLETED] Minister dataset conversion finished."