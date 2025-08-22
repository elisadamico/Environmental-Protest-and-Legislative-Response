## Filtering Tweede Kamer and PSP Data## 
## Written by Katie Nutley from modified code by Elisa D'Amico (Cleaning & Preprocessing/Initial Filtering/climatefiltering_sample.R) 
## Created on: 22 August 2025
## ParlSpeech Data available https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/L4OAKN/PCYUNY&version=1.0

library(data.table)  # For fast data manipulation
library(stringr)  # For string matching
library(furrr)  # For parallel processing

##################################################################################
## THE NETHERLANDS ## 

# Load Dutch data & filter date to 2000-2020
nl_df <- readRDS("~/Downloads/Corp_TweedeKamer_V2.rds")
nl_df$date <- as.Date(nl_df$date)
nl_df <- nl_df[nl_df$date >= "2000-01-01" & nl_df$date <= "2020-12-31", ]

# Load Dutch key words 
nl_keywords <- c(
  # General Climate Change and Environment
  "klimaatverandering", "opwarming van de aarde", "klimaatcrisis", "klimaatnoodtoestand", "klimaatafbraak",
  "klimaatverstoring", "klimaatinstabiliteit", "klimaatramp", "klimatologische verschuivingen",
  "klimaatvariabiliteit", "klimaatwetenschap", "veranderend klimaat", "antropogene klimaatverandering",
  "klimaat-geïnduceerde verandering", "klimaatgerelateerde risico's",
  
  # Greenhouse Gases and Emissions
  "broeikasgassen", "BKG-uitstoot", "koolstofuitstoot", "koolstofvoetafdruk",
  "koolstofneutraliteit", "koolstofvastlegging", "koolstofcompensatie", "koolstofput",
  "koolstofprijsstelling", "koolstofheffing", "koolstofcredit", "koolstofhandel", "methaanuitstoot",
  "CO2-reductie", "netto nul", "koolstofvrije economie", "koolstofarme economie",
  
  # Climate Policy and Governance
  "klimaatactie", "klimaatbeleid", "klimaatbestuur", "klimaataanpassingsbeleid",
  "klimaatmitigatiebeleid", "milieubeleid", "klimaatdiplomatie",
  "internationale klimaatovereenkomsten", "klimaatonderhandelingen", "milieuverdragen",
  "UNFCCC", "Akkoord van Parijs", "COP", "Protocol van Kyoto", "IPCC", "klimaattoezeggingen",
  "nationaal bepaalde bijdragen", "klimaatdoelstellingen", "duurzaam beleid",
  
  # Adaptation and Mitigation
  "klimaataanpassing", "klimaatbestendigheid", "veerkrachtplanning", "klimaatbestendig maken",
  "klimaatmitigatie", "rampenparaatheid", "rampenrisicovermindering", "DRR",
  "rampenbestendigheid", "risicodreduceringsstrategieën", "risicoadaptatie",
  "ecosysteemgebaseerde aanpassing", "natuurgebaseerde oplossingen", "adaptief vermogen",
  "gemeenschapsgebaseerde aanpassing", "kwetsbaarheidsvermindering",
  
  # Renewable Energy and Clean Tech
  "hernieuwbare energie", "schone energie", "zonne-energie", "windenergie", "waterkracht",
  "geothermische energie", "bio-energie", "getijdenenergie", "golfenergie", "duurzame energie",
  "energietransitie", "groene energie", "koolstofarme energie", "off-grid oplossingen",
  "energie-efficiëntie", "schone technologie", "groene technologie", "energiebesparing",
  
  # Fossil Fuels and Decarbonization
  "uitfasering fossiele brandstoffen", "uitfasering kolen", "olie-afhankelijkheid", "fossiele brandstof desinvestering",
  "decarbonisatie", "overgang weg van fossiele brandstoffen", "energie decarbonisatie",
  "schoon vervoer", "elektrische voertuigen", "groen vervoer", "waterstofeconomie",
  
  # Biodiversity, Conservation, and Land Use
  "biodiversiteit", "biodiversiteitsverlies", "ecosysteem ineenstorting", "ecosysteemherstel",
  "milieuvernietiging", "ontbossing", "herbebossing", "bebossing",
  "bosbehoud", "landdegradatie", "landgebruikverandering", "habitatvernietiging",
  "wildlifeconservatie", "mariene bescherming", "oceaanbescherming", "koraalverbleking",
  "oceaanverzuring", "plastic vervuiling", "duurzaam bosbeheer", "landherstel",
  
  # Loss and Damage
  "verlies en schade", "klimaat-geïnduceerd verlies", "klimaat-geïnduceerde schade", "compensatie voor verlies en schade",
  "klimaatreparaties", "klimaatrisicoverzekering", "financiële mechanismen voor klimaatverlies",
  
  # Climate Finance and Economic Policies
  "klimaatfinanciering", "groene financiering", "klimaatinvestering", "duurzame investering",
  "groene obligaties", "koolstofbelasting", "klimaatfondsen", "financiële instrumenten voor klimaatactie",
  "cap en handel", "emissiehandelssysteem", "ETS", "milieubelasting",
  "duurzame ontwikkelingsfinanciering", "groene economie", "circulaire economie",
  "duurzame toeleveringsketens", "klimaatbestendige infrastructuur", "groene stimulus",
  
  # Extreme Weather and Disasters
  "natuurramp", "klimaatramp", "milieurramp", "hittegolven",
  "droogtemitigatie", "bosbradvoor­coming", "overstromingsbeheer", "stormvloeden",
  "zeespiegelstijging", "extreem weer", "orkaan­bestendigheid", "tyfoonparaatheid",
  "tornado­risicobeheer", "moessonvariabiliteit", "weergerelateerde ontheemding",
  
  # Climate-Induced Migration and Social Impacts
  "klimaatvluchtelingen", "milieumigranten", "ontheemding door klimaatverandering",
  "gedwongen migratie", "klimaatgedreven ontheemding", "klimaatrechtvaardigheid", "klimaatgelijkheid",
  "milieurechtvaardigheid", "rechtvaardige overgang", "eerlijke overgang", "klimaatrechten",
  
  # Agriculture, Water Security, and Food Systems
  "duurzame landbouw", "klimaatslimme landbouw", "regeneratieve landbouw",
  "voedselzekerheid", "waterzekerheid", "droogtebestendigheid", "overstromingsbestendige gewassen",
  "agro-ecologie", "bodembehoud", "duurzame visserij", "oceaanbestuur",
  "waterbesparing", "klimaatbestendige landbouw",
  
  # Environmental Regulations and Legal Frameworks
  "milieuregelgeving", "groene wetten", "klimaatrechtspraak", "klimaatrechtszaken",
  "milieubeschermingswetten", "vervuiler betaalt principe", "milieuaansprakelijkheid",
  "bedrijfsmatige milieuverantwoordelijkheid", "netto-nul verbintenissen", "juridisch bindende klimaatdoelen",
  
  # Activism, Movements, and Awareness
  "klimaatactivisme", "milieuactivisme", "jeugdklimaatbeweging",
  "grassroots klimaatactie", "maatschappelijke klimaatbetrokkenheid", "klimaatprotesten",
  "klimaatverdediging", "publiek bewustzijn over klimaatverandering", "mediaverslaggeving over klimaat",
  
  # Innovation and Future Technologies
  "klimaatinnovatie", "koolstofvastlegging en -opslag", "directe luchtvastlegging",
  "geo-engineering", "zonnestralingbeheer", "klimaattechnologie", "klimaatpositieve oplossingen",
  "blauwe koolstof", "negatieve emissietechnologie", "klimaatmonitoringsystemen",
  
  # Miscellaneous Key Phrases
  "milieuverantwoordelijkheid", "groene overgang", "duurzaamheidsstrategieën",
  "ecologisch evenwicht", "natuurlijk kapitaal", "regeneratieve praktijken", "planetaire grenzen",
  "milieubeheer", "ecologische voetafdruk", "milieurisicobeheer"
)

# Word and phrase filtering function
contains_nl_keyword <- function(text, nl_keywords) {
  str_detect(text, regex(str_c(nl_keywords, collapse = "|"), ignore_case = TRUE))
}

# Run filtering in parallel and keep only matching rows
nl_df$match <- future_map_lgl(nl_df$text, ~contains_nl_keyword(.x, nl_keywords))
filtered_nl_df <- nl_df[nl_df$match == TRUE, ]
filtered_nl_df$match <- NULL

View(filtered_nl_df)

# Export filtered Dutch data
saveRDS(filtered_nl_df, "filtered_NL.rds")

################################################################################
## CZECH REPUBLIC ## 


# Load Czech data & filter date to 2000-2020
cz_df <- readRDS("~/Downloads/Corp_PSP_V2.rds")
cz_df$date <- as.Date(cz_df$date)
cz_df <- cz_df[cz_df$date >= "2000-01-01" & cz_df$date <= "2020-12-31", ]

# Load Czech key words
es_keywords <- c(
  # General Climate Change and Environment
  "změna klimatu", "globální oteplování", "klimatická krize", "klimatická nouze", "klimatický kolaps",
  "klimatické narušení", "klimatická nestabilita", "klimatická katastrofa", "klimatické posuny",
  "klimatická variabilita", "klimatická věda", "měnící se klima", "antropogenní změna klimatu",
  "klimaticky podmíněné změny", "klimatická rizika",
  
  # Greenhouse Gases and Emissions
  "skleníkové plyny", "emise skleníkových plynů", "uhlíkové emise", "uhlíková stopa",
  "uhlíková neutralita", "sekvestrace uhlíku", "kompenzace uhlíku", "uhlíkový úložiště",
  "oceňování uhlíku", "uhlíková daň", "uhlíkové kredity", "obchodování s uhlíkem", "emise metanu",
  "snižování CO2", "čistá nula", "bezuhlíková ekonomika", "nízkouhlíková ekonomika",
  
  # Climate Policy and Governance
  "klimatická akce", "klimatická politika", "klimatické řízení", "politika adaptace na klima",
  "politika zmírňování klimatu", "environmentální politika", "klimatická diplomacie",
  "mezinárodní klimatické dohody", "klimatická jednání", "environmentální smlouvy",
  "UNFCCC", "Pařížská dohoda", "COP", "Kjótský protokol", "IPCC", "klimatické závazky",
  "národně stanovené příspěvky", "klimatické cíle", "udržitelné politiky",
  
  # Adaptation and Mitigation
  "adaptace na klima", "klimatická odolnost", "plánování odolnosti", "klimatické zabezpečení",
  "zmírňování klimatu", "připravenost na katastrofy", "snižování rizika katastrof", "DRR",
  "odolnost vůči katastrofám", "strategie snižování rizik", "adaptace na rizika",
  "ekosystémová adaptace", "přírodní řešení", "adaptační kapacita",
  "komunitní adaptace", "snižování zranitelnosti",
  
  # Renewable Energy and Clean Tech
  "obnovitelná energie", "čistá energie", "solární energie", "větrná energie", "vodní energie",
  "geotermální energie", "bioenergie", "přílivová energie", "vlnová energie", "udržitelná energie",
  "energetická transformace", "zelená energie", "nízkouhlíková energie", "off-grid řešení",
  "energetická efektivnost", "čistá technologie", "zelená technologie", "úspora energie",
  
  # Fossil Fuels and Decarbonization
  "postupné ukončení fosilních paliv", "postupné ukončení uhlí", "závislost na ropě", "divestice z fosilních paliv",
  "dekarbonizace", "přechod od fosilních paliv", "energetická dekarbonizace",
  "čistá doprava", "elektrická vozidla", "zelená doprava", "vodíková ekonomika",
  
  # Biodiversity, Conservation, and Land Use
  "biodiverzita", "úbytek biodiverzity", "kolaps ekosystému", "obnova ekosystému",
  "degradace životního prostředí", "odlesňování", "zalesňování", "zalesnění",
  "ochrana lesů", "degradace půdy", "změna využití půdy", "ničení habitatů",
  "ochrana volně žijících živočichů", "ochrana moří", "ochrana oceánů", "bělení korálů",
  "okyselování oceánů", "znečištění plasty", "udržitelné lesnictví", "obnova půdy",
  
  # Loss and Damage
  "ztráty a škody", "klimaticky podmíněné ztráty", "klimaticky podmíněné škody", "kompenzace za ztráty a škody",
  "klimatické reparace", "pojištění klimatických rizik", "finanční mechanismy pro klimatické ztráty",
  
  # Climate Finance and Economic Policies
  "klimatické financování", "zelené financování", "klimatické investice", "udržitelné investice",
  "zelené dluhopisy", "uhlíková daň", "klimatické fondy", "finanční nástroje pro klimatickou akci",
  "cap and trade", "systém obchodování s emisemi", "ETS", "environmentální zdanění",
  "financování udržitelného rozvoje", "zelená ekonomika", "cirkulární ekonomika",
  "udržitelné dodavatelské řetězce", "klimaticky odolná infrastruktura", "zelený stimul",
  
  # Extreme Weather and Disasters
  "přírodní katastrofa", "klimatická katastrofa", "environmentální katastrofa", "vlny veder",
  "zmírňování sucha", "prevence lesních požárů", "řízení povodní", "bouřkové vlny",
  "stoupání hladiny moře", "extrémní počasí", "odolnost vůči hurikánům", "připravenost na tajfuny",
  "řízení rizika tornád", "variabilita monsunů", "vysídlení kvůli počasí",
  
  # Climate-Induced Migration and Social Impacts
  "klimatičtí uprchlíci", "environmentální migranti", "vysídlení kvůli změně klimatu",
  "nucená migrace", "klimaticky podmíněné vysídlení", "klimatická spravedlnost", "klimatická rovnost",
  "environmentální spravedlnost", "spravedlivá transformace", "férová transformace", "klimatická práva",
  
  # Agriculture, Water Security, and Food Systems
  "udržitelné zemědělství", "klimaticky chytré zemědělství", "regenerativní zemědělství",
  "potravinová bezpečnost", "vodní bezpečnost", "odolnost vůči suchu", "povodnítolerantní plodiny",
  "agroekologie", "ochrana půdy", "udržitelný rybolov", "správa oceánů",
  "hospodaření s vodou", "klimaticky odolné zemědělství",
  
  # Environmental Regulations and Legal Frameworks
  "environmentální regulace", "zelené zákony", "klimatická soudní řízení", "klimatické žaloby",
  "zákony o ochraně životního prostředí", "princip znečišťovatel platí", "environmentální odpovědnost",
  "podniková environmentální odpovědnost", "závazky čisté nuly", "právně závazné klimatické cíle",
  
  # Activism, Movements, and Awareness
  "klimatický aktivismus", "environmentální aktivismus", "mládežnické klimatické hnutí",
  "grassroots klimatická akce", "občanské angažování v klimatu", "klimatické protesty",
  "klimatické obhajoba", "veřejné povědomí o změně klimatu", "mediální pokrytí klimatu",
  
  # Innovation and Future Technologies
  "klimatické inovace", "zachycování a ukládání uhlíku", "přímé zachycování ze vzduchu",
  "geoengineering", "řízení slunečního záření", "klimatické technologie", "klimaticky pozitivní řešení",
  "modrý uhlík", "technologie negativních emisí", "systémy monitorování klimatu",
  
  # Miscellaneous Key Phrases
  "environmentální odpovědnost", "zelená transformace", "strategie udržitelnosti",
  "ekologická rovnováha", "přírodní kapitál", "regenerativní praktiky", "planetární hranice",
  "environmentální správcovství", "ekologická stopa", "řízení environmentálních rizik"
)

# Word and phrase filtering function
contains_cz_keyword <- function(text, cz_keywords) {
  str_detect(text, regex(str_c(cz_keywords, collapse = "|"), ignore_case = TRUE))
}

# Run filtering in parallel and keep only matching rows
cz_df$match <- future_map_lgl(cz_df$text, ~contains_cz_keyword(.x, cz_keywords))
filtered_cz_df <- cz_df[cz_df$match == TRUE, ]
filtered_cz_df$match <- NULL

View(filtered_cz_df)

# Export filtered Czech data
saveRDS(filtered_cz_df, "filtered_CZ.rds")

################################################################################
## SPAIN ##

# Load Spanish data & filter date to 2000-2020
es_df <- readRDS("~/Downloads/Corp_Congreso_V2.rds")
es_df$date <- as.Date(es_df$date)
es_df <- es_df[es_df$date >= "2000-01-01" & es_df$date <= "2020-12-31", ]

# Load Spanish key words
es_keywords <- c(
  # General Climate Change and Environment
  "cambio climático", "calentamiento global", "crisis climática", "emergencia climática", "colapso climático",
  "alteración climática", "inestabilidad climática", "catástrofe climática", "cambios climáticos",
  "variabilidad climática", "ciencia climática", "clima cambiante", "cambio climático antropogénico",
  "cambio inducido por el clima", "riesgos relacionados con el clima",
  
  # Greenhouse Gases and Emissions
  "gases de efecto invernadero", "emisiones de GEI", "emisiones de carbono", "huella de carbono",
  "neutralidad de carbono", "secuestro de carbono", "compensación de carbono", "sumidero de carbono",
  "precio del carbono", "impuesto al carbono", "créditos de carbono", "comercio de carbono", "emisiones de metano",
  "reducción de CO2", "cero neto", "economía libre de carbono", "economía baja en carbono",
  
  # Climate Policy and Governance
  "acción climática", "política climática", "gobernanza climática", "política de adaptación climática",
  "política de mitigación climática", "política ambiental", "diplomacia climática",
  "acuerdos climáticos internacionales", "negociaciones climáticas", "tratados ambientales",
  "CMNUCC", "Acuerdo de París", "COP", "Protocolo de Kioto", "IPCC", "compromisos climáticos",
  "contribuciones determinadas a nivel nacional", "objetivos climáticos", "políticas sostenibles",
  
  # Adaptation and Mitigation
  "adaptación climática", "resiliencia climática", "planificación de resiliencia", "climatización",
  "mitigación climática", "preparación para desastres", "reducción del riesgo de desastres", "RRD",
  "resiliencia ante desastres", "estrategias de reducción de riesgo", "adaptación al riesgo",
  "adaptación basada en ecosistemas", "soluciones basadas en la naturaleza", "capacidad adaptativa",
  "adaptación comunitaria", "reducción de vulnerabilidad",
  
  # Renewable Energy and Clean Tech
  "energía renovable", "energía limpia", "energía solar", "energía eólica", "energía hidroeléctrica",
  "energía geotérmica", "bioenergía", "energía mareomotriz", "energía undimotriz", "energía sostenible",
  "transición energética", "energía verde", "energía baja en carbono", "soluciones autónomas",
  "eficiencia energética", "tecnología limpia", "tecnología verde", "conservación de energía",
  
  # Fossil Fuels and Decarbonization
  "eliminación gradual de combustibles fósiles", "eliminación gradual del carbón", "dependencia del petróleo", "desinversión en combustibles fósiles",
  "descarbonización", "transición desde combustibles fósiles", "descarbonización energética",
  "transporte limpio", "vehículos eléctricos", "transporte verde", "economía del hidrógeno",
  
  # Biodiversity, Conservation, and Land Use
  "biodiversidad", "pérdida de biodiversidad", "colapso de ecosistemas", "restauración de ecosistemas",
  "degradación ambiental", "deforestación", "reforestación", "forestación",
  "conservación forestal", "degradación del suelo", "cambio de uso del suelo", "destrucción de hábitats",
  "conservación de vida silvestre", "protección marina", "conservación oceánica", "blanqueamiento de corales",
  "acidificación oceánica", "contaminación por plásticos", "silvicultura sostenible", "restauración de tierras",
  
  # Loss and Damage
  "pérdidas y daños", "pérdidas inducidas por el clima", "daños inducidos por el clima", "compensación por pérdidas y daños",
  "reparaciones climáticas", "seguro de riesgo climático", "mecanismos financieros para pérdidas climáticas",
  
  # Climate Finance and Economic Policies
  "financiación climática", "financiación verde", "inversión climática", "inversión sostenible",
  "bonos verdes", "impuesto al carbono", "fondos climáticos", "instrumentos financieros para acción climática",
  "tope y intercambio", "sistema de comercio de emisiones", "SCE", "tributación ambiental",
  "financiación del desarrollo sostenible", "economía verde", "economía circular",
  "cadenas de suministro sostenibles", "infraestructura resiliente al clima", "estímulo verde",
  
  # Extreme Weather and Disasters
  "desastre natural", "desastre climático", "desastre ambiental", "olas de calor",
  "mitigación de sequía", "prevención de incendios forestales", "gestión de inundaciones", "marejadas ciclónicas",
  "aumento del nivel del mar", "clima extremo", "resiliencia ante huracanes", "preparación para tifones",
  "gestión del riesgo de tornados", "variabilidad del monzón", "desplazamiento relacionado con el clima",
  
  # Climate-Induced Migration and Social Impacts
  "refugiados climáticos", "migrantes ambientales", "desplazamiento por cambio climático",
  "migración forzada", "desplazamiento impulsado por el clima", "justicia climática", "equidad climática",
  "justicia ambiental", "transición justa", "transición equitativa", "derechos climáticos",
  
  # Agriculture, Water Security, and Food Systems
  "agricultura sostenible", "agricultura climáticamente inteligente", "agricultura regenerativa",
  "seguridad alimentaria", "seguridad hídrica", "resiliencia a la sequía", "cultivos resistentes a inundaciones",
  "agroecología", "conservación del suelo", "pesca sostenible", "gobernanza oceánica",
  "conservación del agua", "agricultura resiliente al clima",
  
  # Environmental Regulations and Legal Frameworks
  "regulaciones ambientales", "leyes verdes", "litigios climáticos", "demandas climáticas",
  "leyes de protección ambiental", "principio de quien contamina paga", "responsabilidad ambiental",
  "responsabilidad ambiental corporativa", "compromisos de cero neto", "objetivos climáticos legalmente vinculantes",
  
  # Activism, Movements, and Awareness
  "activismo climático", "activismo ambiental", "movimiento climático juvenil",
  "acción climática de base", "compromiso de la sociedad civil con el clima", "protestas climáticas",
  "defensa climática", "conciencia pública sobre cambio climático", "cobertura mediática del clima",
  
  # Innovation and Future Technologies
  "innovación climática", "captura y almacenamiento de carbono", "captura directa del aire",
  "geoingeniería", "gestión de radiación solar", "tecnología climática", "soluciones climáticamente positivas",
  "carbono azul", "tecnología de emisiones negativas", "sistemas de monitoreo climático",
  
  # Miscellaneous Key Phrases
  "responsabilidad ambiental", "transición verde", "estrategias de sostenibilidad",
  "equilibrio ecológico", "capital natural", "prácticas regenerativas", "límites planetarios",
  "administración ambiental", "huella ecológica", "gestión de riesgos ambientales"
)

# Word and phrase filtering function
contains_es_keyword <- function(text, es_keywords) {
  str_detect(text, regex(str_c(es_keywords, collapse = "|"), ignore_case = TRUE))
}

# Run filtering in parallel and keep only matching rows
es_df$match <- future_map_lgl(es_df$text, ~contains_es_keyword(.x, es_keywords))
filtered_es_df <- es_df[es_df$match == TRUE, ]
filtered_es_df$match <- NULL

View(filtered_es_df)

# Export filtered Spanish data
saveRDS(filtered_es_df, "filtered_ES.rds")

