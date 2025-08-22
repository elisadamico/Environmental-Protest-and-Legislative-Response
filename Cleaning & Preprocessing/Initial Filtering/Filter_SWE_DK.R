## Filtering Riksdag and Folketing Data## 
## Written by Katie Nutley from modified code by Elisa D'Amico (Cleaning & Preprocessing/Initial Filtering/climatefiltering_sample.R) 
## Created on: 15 August 2025
## ParlSpeech Data available https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/L4OAKN/PCYUNY&version=1.0

library(data.table)  # For fast data manipulation
library(stringr)  # For string matching
library(furrr)  # For parallel processing

################################################################################

## SWEDEN ## 

# Load Swedish data & filter date to 2000-2020
swe_df <- readRDS("~/Downloads/Corp_Riksdagen_V2.rds")
swe_df$date <- as.Date(swe_df$date)
swe_df <- swe_df[swe_df$date >= "2000-01-01" & swe_df$date <= "2020-12-31", ]

# Load Swedish key words 
swe_keywords <- c(
    # General Climate Change and Environment
    "klimatförändring", "global uppvärmning", "klimatkris", "klimatnödläge", "klimatkollaps",
    "klimatstörning", "klimatinstabilitet", "klimatkatastof", "klimatskiften",
    "klimatvariabilitet", "klimatvetenskap", "föränderligt klimat", "antropogen klimatförändring",
    "klimatorsakad förändring", "klimatrelaterade risker",
    
    # Greenhouse Gases and Emissions
    "växthusgaser", "växthusgasutsläpp", "koldioxidutsläpp", "koldioxidavtryck",
    "koldioxidneutralitet", "kolbindning", "koldioxidkompensation", "kolsänka",
    "koldioxidprissättning", "koldioxidavgift", "koldioxidkredit", "koldioxidhandel", "metanutsläpp",
    "CO2-minskning", "netto noll", "koldioxidfri ekonomi", "koldioxidsnål ekonomi",
    
    # Climate Policy and Governance
    "klimatåtgärder", "klimatpolitik", "klimatstyrning", "klimatanpassningspolitik",
    "klimatbegränsningspolitik", "miljöpolitik", "klimatdiplomati",
    "internationella klimatavtal", "klimatförhandlingar", "miljöfördrag",
    "UNFCCC", "Parisavtalet", "COP", "Kyotoprotokollet", "IPCC", "klimatlöften",
    "nationellt fastställda bidrag", "klimatmål", "hållbara policies",
    
    # Adaptation and Mitigation
    "klimatanpassning", "klimatmotståndskraft", "motståndskraftsplanering", "klimatsäkring",
    "klimatbegränsning", "katastrofberedskap", "katastrofriskreducering", "KRR",
    "katastrof-motståndskraft", "riskminskningsstrategier", "riskanpassning",
    "ekosystembaserad anpassning", "naturbaserade lösningar", "anpassningskapacitet",
    "samhällsbaserad anpassning", "sårbarhetsminskning",
    
    # Renewable Energy and Clean Tech
    "förnybar energi", "ren energi", "solenergi", "vindkraft", "vattenkraft",
    "geotermisk energi", "bioenergi", "tidvattenenergi", "vågenergi", "hållbar energi",
    "energiomställning", "grön energi", "koldioxidsnål energi", "off-grid lösningar",
    "energieffektivitet", "ren teknik", "grön teknik", "energibesparing",
    
    # Fossil Fuels and Decarbonization
    "utfasning av fossila bränslen", "utfasning av kol", "oljeberoende", "divestering från fossila bränslen",
    "avkarbonisering", "övergång bort från fossila bränslen", "energiavkarbonisering",
    "ren transport", "elfordon", "grön transport", "vätgasekonomi",
    
    # Biodiversity, Conservation, and Land Use
    "biologisk mångfald", "förlust av biologisk mångfald", "ekosystemkollaps", "ekosystemrestaurering",
    "miljöförstöring", "avskogning", "återbeskogning", "nyplantering av skog",
    "skogsbevarande", "markförstöring", "markanvändningsförändring", "habitatförstöring",
    "viltskydd", "marina skydd", "havsbevarande", "korallblekning",
    "havsförsurning", "plastföroreningar", "hållbart skogsbruk", "markrestaurering",
    
    # Loss and Damage
    "förlust och skada", "klimatorsakad förlust", "klimatorsakad skada", "kompensation för förlust och skada",
    "klimatreparationer", "klimatriskförsäkring", "finansiella mekanismer för klimatförluster",
    
    # Climate Finance and Economic Policies
    "klimatfinansiering", "grön finansiering", "klimatinvestering", "hållbar investering",
    "gröna obligationer", "koldioxidskatt", "klimatfonder", "finansiella instrument för klimatåtgärder",
    "tak och handel", "utsläppshandelssystem", "ETS", "miljöbeskattning",
    "finansiering av hållbar utveckling", "grön ekonomi", "cirkulär ekonomi",
    "hållbara leveranskedjor", "klimatmotståndskraftig infrastruktur", "grön stimulans",
    
    # Extreme Weather and Disasters
    "naturkatastrof", "klimatkatastrof", "miljökatastrof", "värmeböljor",
    "torka-begränsning", "skogsbrandsprevention", "översvämningshantering", "stormfloder",
    "havsnivåhöjning", "extremväder", "orkanmotståndskraft", "tyfon-beredskap",
    "tornado-riskhantering", "monsuners variabilitet", "väderrelaterad förflyttning",
    
    # Climate-Induced Migration and Social Impacts
    "klimatflyktingar", "miljömigranter", "förflyttning på grund av klimatförändring",
    "tvångsflyttning", "klimatdriven förflyttning", "klimaträttvisa", "klimaträttvisa",
    "miljörättvisa", "rättvis omställning", "rättvis övergång", "klimaträttigheter",
    
    # Agriculture, Water Security, and Food Systems
    "hållbart jordbruk", "klimatsmart jordbruk", "regenerativt jordbruk",
    "livsmedelssäkerhet", "vattensäkerhet", "torkamotståndskraft", "översvämningsresistenta grödor",
    "agroekologi", "markbevarande", "hållbart fiske", "havsstyrning",
    "vattenbevarande", "klimatmotståndskraftigt jordbruk",
    
    # Environmental Regulations and Legal Frameworks
    "miljöbestämmelser", "gröna lagar", "klimaträttsprocesser", "klimatstämningar",
    "miljöskyddslagar", "principen att förorenaren betalar", "miljöansvar",
    "företags miljöansvar", "netto-noll åtaganden", "juridiskt bindande klimatmål",
    
    # Activism, Movements, and Awareness
    "klimataktivism", "miljöaktivism", "ungdomsklimatrörelse",
    "gräsrots-klimatåtgärder", "civilsamhällets klimatengagemang", "klimatprotester",
    "klimatförespråkande", "allmän medvetenhet om klimatförändring", "mediebevakning av klimat",
    
    # Innovation and Future Technologies
    "klimatinnovation", "koldioxidinfångning och lagring", "direkt luftinfångning",
    "geoteknik", "hantering av solstrålning", "klimatteknik", "klimatpositiva lösningar",
    "blått kol", "teknik för negativa utsläpp", "klimatövervakningssystem",
    
    # Miscellaneous Key Phrases
    "miljöansvar", "grön omställning", "hållbarhetsstrategier",
    "ekologisk balans", "naturkapital", "regenerativa metoder", "planetära gränser",
    "miljöförvaltning", "ekologiskt avtryck", "miljöriskhantering"
  )

# Word and phrase filtering function
contains_swe_keyword <- function(text, swe_keywords) {
  str_detect(text, regex(str_c(swe_keywords, collapse = "|"), ignore_case = TRUE))
}

# Run filtering in parallel and keep only matching rows
swe_df$match <- future_map_lgl(swe_df$text, ~contains_swe_keyword(.x, swe_keywords))
filtered_swe_df <- swe_df[swe_df$match == TRUE, ]
filtered_swe_df$match <- NULL

View(filtered_swe_df)

# Export filtered Swedish data
saveRDS(filtered_swe_df, "filtered_SWE.rds")

################################################################################

## DENMARK ## 

# Load Danish data & filter date to 2000-2020
dk_df <- readRDS("~/Downloads/Corp_Folketing_V2.rds")
dk_df$date <- as.Date(dk_df$date)
dk_df <- dk_df[dk_df$date >= "2000-01-01" & dk_df$date <= "2020-12-31", ]

# Load Swedish key words 
dk_keywords <- c(
  # General Climate Change and Environment
  "klimaforandring", "global opvarmning", "klimakrise", "klimanødsituation", "klimakollaps",
  "klimaforstyrrelser", "klimaustabilitet", "klimakatastrofe", "klimaskift",
  "klimavariabilitet", "klimavidenskab", "foranderligt klima", "menneskeskabte klimaforandringer",
  "klimainducerede forandringer", "klimarelaterede risici",
  
  # Greenhouse Gases and Emissions
  "drivhusgasser", "drivhusgasudledninger", "CO2-udledninger", "CO2-fodaftryk",
  "CO2-neutralitet", "kulstofbinding", "CO2-kompensation", "kulstoflagring",
  "CO2-prissætning", "CO2-afgift", "CO2-kreditter", "CO2-handel", "metanudledninger",
  "CO2-reduktion", "netto nul", "nul-kulstof økonomi", "lavkulstof økonomi",
  
  # Climate Policy and Governance
  "klimahandling", "klimapolitik", "klimastyring", "klimatilpasningspolitik",
  "klimaafbødningspolitik", "miljøpolitik", "klimadiplomati",
  "internationale klimaaftaler", "klimaforhandlinger", "miljøtraktater",
  "UNFCCC", "Parisaftalen", "COP", "Kyoto-protokollen", "IPCC", "klimatilsagn",
  "nationalt fastsatte bidrag", "klimamål", "bæredygtige politikker",
  
  # Adaptation and Mitigation
  "klimatilpasning", "klimamodstandsdygtighed", "modstandsdygtighedsplanlægning", "klimasikring",
  "klimaafbødning", "katastrofeberedskab", "katastroferisikoreduktion", "KRR",
  "katastrofemodstandsdygtighed", "risikoreduktionsstrategier", "risikotilpasning",
  "økosystembaseret tilpasning", "naturbaserede løsninger", "tilpasningskapacitet",
  "samfundsbaseret tilpasning", "sårbarhedsreduktion",
  
  # Renewable Energy and Clean Tech
  "vedvarende energi", "ren energi", "solenergi", "vindkraft", "vandkraft",
  "geotermisk energi", "bioenergi", "tidevandsenergi", "bølgeenergi", "bæredygtig energi",
  "energiomstilling", "grøn energi", "lavkulstof energi", "off-grid løsninger",
  "energieffektivitet", "ren teknologi", "grøn teknologi", "energibesparelse",
  
  # Fossil Fuels and Decarbonization
  "udfasning af fossile brændstoffer", "udfasning af kul", "olieafhængighed", "frasalg af fossile brændstoffer",
  "afkulstofning", "omstilling væk fra fossile brændstoffer", "energiafkulstofning",
  "ren transport", "elektriske køretøjer", "grøn transport", "brintøkonomi",
  
  # Biodiversity, Conservation, and Land Use
  "biodiversitet", "tab af biodiversitet", "økosystemkollaps", "økosystemgenopretning",
  "miljøforringelse", "skovrydning", "genopbygning af skove", "nyplantning af skove",
  "skovbevarelse", "jordforringelse", "ændring i arealanvendelse", "habitatødelæggelse",
  "naturbeskyttelse", "marinebeskyttelse", "havbevarelse", "koralblegning",
  "havforsuring", "plastforurening", "bæredygtigt skovbrug", "jordgenopretning",
  
  # Loss and Damage
  "tab og skade", "klimainduceret tab", "klimainduceret skade", "kompensation for tab og skade",
  "klimareparationer", "klimarisikoforsikring", "finansielle mekanismer for klimatab",
  
  # Climate Finance and Economic Policies
  "klimafinansiering", "grøn finansiering", "klimainvestering", "bæredygtig investering",
  "grønne obligationer", "CO2-skat", "klimafonde", "finansielle instrumenter for klimahandling",
  "loft og handel", "emissionshandelssystem", "ETS", "miljøbeskatning",
  "bæredygtig udviklingsfinansiering", "grøn økonomi", "cirkulær økonomi",
  "bæredygtige forsyningskæder", "klimamodstandsdygtig infrastruktur", "grøn stimulans",
  
  # Extreme Weather and Disasters
  "naturkatastrofe", "klimakatastrofe", "miljøkatastrofe", "hedebølger",
  "tørkeafbødning", "skovbrandforebyggelse", "oversvømmelseshåndtering", "stormfloder",
  "havniveaustigning", "ekstremvejr", "orkanmodstandsdygtighed", "tyfonforberedelse",
  "tornadorisikohåndtering", "monsunvariabilitet", "vejrrelateret forflytning",
  
  # Climate-Induced Migration and Social Impacts
  "klimaflygtninge", "miljømigranter", "forflytning på grund af klimaforandringer",
  "tvangsmigrering", "klimadrevet forflytning", "klimaretfærdighed", "klimaligestilling",
  "miljøretfærdighed", "retfærdig omstilling", "fair omstilling", "klimarettigheder",
  
  # Agriculture, Water Security, and Food Systems
  "bæredygtigt landbrug", "klimasmart landbrug", "regenerativt landbrug",
  "fødevaresikkerhed", "vandsikkerhed", "tørkemodstandsdygtighed", "oversvømmelsesresistente afgrøder",
  "agroøkologi", "jordbevarelse", "bæredygtigt fiskeri", "havstyring",
  "vandbevarelse", "klimamodstandsdygtigt landbrug",
  
  # Environmental Regulations and Legal Frameworks
  "miljøreguleringer", "grønne love", "klimasøgsmål", "klimasager",
  "miljøbeskyttelseslove", "princippet om at forureneren betaler", "miljøansvar",
  "virksomheders miljøansvar", "netto-nul forpligtelser", "juridisk bindende klimamål",
  
  # Activism, Movements, and Awareness
  "klimaaktivisme", "miljøaktivisme", "ungdomsklimabevaegelse",
  "græsrods klimahandling", "civilsamfundets klimaengagement", "klimaprotester",
  "klimafortaler", "offentlig bevidsthed om klimaforandringer", "mediedækning af klima",
  
  # Innovation and Future Technologies
  "klimainnovation", "kulstofopsamling og -lagring", "direkte luftopsamling",
  "geoengineering", "solstrålingstyring", "klimateknologi", "klimapositive løsninger",
  "blå kulstof", "negativ emissionsteknologi", "klimaovervågningssystemer",
  
  # Miscellaneous Key Phrases
  "miljøansvar", "grøn omstilling", "bæredygtighedsstrategier",
  "økologisk balance", "naturkapital", "regenerative praksisser", "planetære grænser",
  "miljøforvaltning", "økologisk fodaftryk", "miljørisikostyring"
)

# Word and phrase filtering function
contains_dk_keyword <- function(text, dk_keywords) {
  str_detect(text, regex(str_c(dk_keywords, collapse = "|"), ignore_case = TRUE))
}

# Run filtering in parallel and keep only matching rows
dk_df$match <- future_map_lgl(dk_df$text, ~contains_dk_keyword(.x, dk_keywords))
filtered_dk_df <- dk_df[dk_df$match == TRUE, ]
filtered_dk_df$match <- NULL

View(filtered_dk_df)

# Export filtered Swedish data
saveRDS(filtered_dk_df, "filtered_DK.rds")





