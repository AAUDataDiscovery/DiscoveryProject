import pandas as pd
import numpy as np
import random
import datetime
from random import gauss
from numpy.random import seed
from numpy.random import rand

country = ['Denmark', 'Sweden', 'Norway', 'Finland', 'Germany','France','Spain','Italy','Netherlands','Belgium','Poland','Switzerland','Austria','Romania','Hungary','Lithuania','Estonia','Croatia','Greece','Turkey']

city = {0:['Aalborg', 'Copenhagen', 'Aarhus', 'Silkeborg', 'Odense','Esbjerg','Randers','Kolding','Horsens','Vejle','Roskilde','Herning','Horsholm','Helsingor','Naesteved','Fredericia','Viborg','Kore','Hillerod','Hjorring'], 
        1:['Stockholm','Gothenburg','Malmo','Uppsala','Vasteras','Orebro','Linkoping','Helsingborg','Jokoping','Norrkoping','Lund','Umea','Gavle','Boras','Halmstad','Vaxjo','Karlstad','Lulea','Kristiansand','Ostersund'],
        2:['Oslo','Bergen','Trondheim','Drammen','Kristiansand','Stavanger','Fredrikstad','Sandnes','Tromso','Skien','Sarpsborg','Sandefjord','Alesund','Larvik','Arendal','Tonsberg','Bodo','Porsgrunn','Haugesund','Hamar'],
        3:['Helsinki','Espoo','Tempere','Vantaa','Oulu','Turku','Jyvaskyla','Lahti','Kuopio','Pori','Kouvola','Joensuu','Lappeenranta','Hameenlinna','Vaasa','Seinajoki','Rovaniemi','Mikkeli','Kotka'],
        4:['Berlin','Hamburg','Frankfurt','Cologne','Stuttgard','Dussedorf','Essen','Leipzig','Bremen','Dresden','Hannover','Nuremberg','Biefeld','Karlsruhe','Münster','Kiel','Aachen','Freiburg','Krefeld','Mainz'],
        5:['Paris','Marseille','Nice','Brest','Strasbourg','Lille','Rennes','Toulouse','Lyon','Nantes','Montpellier','Reims','Le Havre','Saint-Etienne','Toulon','Grenoble','Dijon','Nimes','Angers','Tours'],
        6:['Madrid','Barcelona','Valencia','Malaga','Seville','Betis','Bilbao','Cordoba','Zaragoza','Las Palmas','Vigo','Valladolid','Alicante','Elche','Toledo','Granada','Cadiz','Huelva','Salamanca','Almeria'],
        7:['Rome','Milan','Naples','Turin','Palermo','Genoa','Bologna','Florence','Bari','Catania','Verona','Venice','Messina','Padua','Prato','Trieste','Brescia','Parma','Taranto','Modena'],
        8:['Amsterdam','Rotterdam','The Hague','Utrecht','Eindhoven','Groningen','Tilburg','Almere','Breda','Nijmegen','Apeldoorn','Arnhem','Haarlem','Enschede','Haarlemmermmer','Amesfoort','Zaanstad','Hertogenbosch','Zwolle','Zoetermmer'],
        9:['Antwerp','Ghent','Charleroi','Liege','Brussels','Bruges','Namur','Leuven','Mons','Mechelen','Aalst','La Louviere','Hasselt','Sint-Niklaas','Kortrijk','Ostend','Tournal','Genk','Seraing','Roeselare'],
        10:['Warsaw','Krakow','Lodz','Wroclaw','Poznan','Gdansk','Sczecin','Bydosczcz','Lublin','Bialystok','Katowice','Gdynia','Czestochowa','Radom','Rzeszow','Torun','Kielce','Olsztyn','Bielsko-Biala','Rybnik'],
        11:['Zurich','Geneva','Basel','Lausanne','Bern','Winterthur','Lucerne','St. Gallen','Lugano','Biel/Bienne','Thun','Bellinzona','Koniz','Fribourg','Schaffhausen','Chur','Uster','Sion','Vernier','Lancy'],
        12:['Vienna','Graz','Linz','Salzburg','Innsbruck','Klagenfurt am Worthersse','Villach','Wels','Sankt Polten','Dornbirn','Wiener Neustadt','Seyr','Feldkirch','Bregenz','Leonding','Klosterneubburg','Baden bei Wien','Wolfsberg','Leoben','Krems an der Donau'],
        13:['Bucharest','Cluj-Napoca','Timisoara','Iasi','Constanta','Craiova','Brasov','Galati','Ploiesti','Oradea','Braila','Arad','Pitesti','Sibiu','Bacau','Targu Mures','Baia Mare','Buzau','Botosani','Targoviste'],
        14:['Budapest','Debrecen','Szeged','Miskolc','Pecs','Gyor','Nyiregyhaza','Kecskemet','Szekesfehervar','Szombathely','Erd','Zolnok','Tabanya','Sozopron','Kaposvar','Bekescsaba','Veszprem','Zalaegerszeg','Eger','Nagykanizsa'],
        15:['Vilnius','Kaunas','Klaipeda','Siauliai','Panevezys','Alytus','Marijampole','Mazeikiai','Jonava','Utena','Kedainiai','Taurage','Telsiai','Ukmerge','Visaginas','Plunge','Kretinga','Palanga','Radvillskis','Silute'],
        16:['Tallinn','Tarfu','Narva','Parnu','Kohtla-Jarve','Vijandi','Rakvere','Maardu','Kuressaare','Sillamae','Valga','Voru','Johvi','Haapsalu','Keila','Paide','Elva','Saue','Polva','Tapa'],
        17:['Zagreb','Split','Rijeka','Osijek','Zadar','Velika Gorica','Pula','Slavonski Brod','Karlovac','Varazdin','Sibenik','Dubrovnik','Sisak','Kastela','Samobor','Bjelovar','Vinkovci','Koprivnica','Cakovec','Solin'],
        18:['Athens','Thessaloniki','Patras','Piraeus','Larissa','Heraklion','Peristeri','Kallithea','Acharnes','Kalamaria','Nikaia','Glyfada','Volos','Ilio','Ilioupoli','Keratsini','Evosmos','Chalandri','Nea Smyrni','Marousi'],
        19:['Istanbul','Ankara','Izmir','Bursa','Adana','Gaziantep','Konya','Antalya','Kayseri','Mersin','Eskisehir','Diyarbakir','Samsun','Denizli','Sanliurfa','Adapazan','Malatya','Kahramanmaras','Erzurum','Van'],
}

name = ['Kaylah' ,'Chasity' ,'Giovann' ,'Natalie' ,'Aryanna' ,'Elena' ,'Dax' ,'Dakota' ,'Abby' ,'Ryder' ,'Marco' ,'Kyla' ,'Johanna' ,'Felipe' ,'Kendra' ,'Amelie' ,'Izayah' ,'Kamron' ,'Jovanni' ,'Philip' ,'Izaiah' ,'Aaron' ,'Arthur' ,'Billy' ,'Aniyah' ,'Winston' ,'Shamar' ,'Kara' ,'Denisse' ,'Cora' ,'Edward' ,'Reyna' ,'Felix' ,'Kaeden' ,'Richard' ,'Tristin' ,'Paulina' ,'Quinton' ,'Ignacio' ,'Turner' ,'Jeremy' ,'Trace' ,'Everett' ,'Kobe' ,'Brock' ,'Norah' ,'Tyree' ,'Timothy' ,'Jasmine' ,'Gunnar' ,'Aileen' ,'Marie' ,'Sydney' ,'Mason' ,'Finn' ,'Fisher' ,'Liliana' ,'Kendrick' ,'Cynthia' ,'Neil' ,'Liana' ,'Oswaldo' ,'Bryanna' ,'Alvin' ,'Mariyah' ,'Akira' ,'Averie' ,'Tate' ,'Soren' ,'Ahmed' ,'Frederick' ,'Sage' ,'Armando' ,'Teresa' ,'Kash' ,'Annabella' ,'Destiny' ,'Manuel' ,'Brynn' ,'Anne' ,'Jaden' ,'Esther' ,'Moses' ,'Sidney' ,'Callie' ,'Colten' ,'Urijah' ,'Paxton' ,'Dennis' ,'Draven' ,'Trinity' ,'Elliot' ,'Caroline' ,'Kaydence' ,'Renee' ,'Tyrone' ,'June' ,'Francisco' ,'Walter' ,'Tiffany']

genre = ['Female', 'Male']

job = ['Coach' , 'Musician' , 'Customer Service Representative' , 'Logistician' , 'Art Director' , 'Carpenter' , 'Statistician' , 'Database administrator' , 'Court Reporter' , 'Computer Hardware Engineer' , 'Market Research Analyst' , 'Physicist' , 'Landscape Architect' , 'Registered Nurse' , 'Bookkeeping clerk' , 'Elementary School Teacher' , 'Security Guard' , 'Computer Systems Analyst' , 'High School Teacher' , 'Zoologist' , 'Historian' , 'Architect' , 'Drafter' , 'Paralegal' , 'Psychologist' , 'Educator' , 'Computer Systems Administrator' , 'Substance Abuse Counselor' , 'Teacher Assistant' , 'Editor' , 'Photographer' , 'Secretary' , 'Cost Estimator' , 'Automotive mechanic' , 'Food Scientist' , 'Police Officer' , 'Designer' , 'Epidemiologist' , 'Executive Assistant' , 'Preschool Teacher' , 'Actuary' , 'Budget analyst' , 'Pharmacist' , 'Massage Therapist' , 'Construction Manager' , 'Social Worker' , 'Occupational Therapist' , 'Respiratory Therapist' , 'Mason' , 'Truck Driver' , 'Radiologic Technologist' , 'HR Specialist' , 'Chemist' , 'Human Resources Assistant' , 'Speech-Language Pathologist' , 'Recreational Therapist' , 'Urban Planner' , 'Environmental scientist' , 'Paramedic' , 'Public Relations Specialist' , 'Economist' , 'Reporter' , 'Childcare worker' , 'Dental Hygienist' , 'Painter' , 'Mechanical Engineer' , 'Writer' , 'Chef' , 'Civil Engineer' , 'Referee' , 'Housekeeper' , 'Plumber' , 'Computer Programmer' , 'Professional athlete' , 'Sports Coach' , 'Surveyor' , 'Janitor' , 'Electrician' , 'Actor' , 'Personal Care Aide' , 'Farmer' , 'Landscaper & Groundskeeper' , 'Medical Secretary' , 'Physical Therapist' , 'Mathematician' , 'Receptionist' , 'Recreation & Fitness Worker' , 'Event Planner' , 'Marriage & Family Therapist' , 'Hairdresser' , 'Desktop publisher' , 'Medical Assistant' , 'Telemarketer' , 'School Psychologist' , 'Compliance Officer' , 'Fitness Trainer' , 'Patrol Officer' , 'Auto Mechanic' , 'Real Estate Agent' , 'Physician']

nationality =['Indian' , 'Swede' , 'Pakistani' , 'Rwandan' , 'Colombian' , 'Belorussian' , 'Paraguayan' , 'Canadian' , 'Azerbaijani' , 'Dane' , 'Kenyan' , 'Namibian' , 'Maldivian' , 'Luxembourger' , 'Liberian' , 'Montenegrin' , 'Singaporean' , 'Zambian' , 'Croatian' , 'Ecuadorean' , 'Egyptian' , 'Paraguayan' , 'Iranian' , 'Togolese' , 'Madagascan' , 'Panamanian' , 'Afghan' , 'Mauritanian' , 'Singaporean' , 'Turk' , 'Fijian' , 'Mongolian' , 'Mozambican' , 'Monacan' , 'Israeli' , 'Bosnian' , 'Paraguayan' , 'Guatemalan' , 'Chadian' , 'Serbian' , 'Lebanese' , 'Italian' , 'Congolese' , 'Chinese' , 'Paraguayan' , 'Uruguayan' , 'Mauritanian' , 'Liberian' , 'Iraqi' , 'Bangladeshi' , 'Greek' , 'Cambodian' , 'Ugandan' , 'Ethiopian' , 'Swiss' , 'Canadian' , 'Grenadian' , 'Argentinian' , 'Latvian' , 'Ugandan' , 'American' , 'Bosnian' , 'Nepalese' , 'Russian' , 'Jamaican' , 'Liberian' , 'Moroccan' , 'Senegalese' , 'Iraqi' , 'Liberian' , 'Japanese' , 'Mauritanian' , 'Jordanian' , 'American' , 'Macedonian' , 'Namibian' , 'Bolivian' , 'Bangladeshi' , 'Chinese' , 'Yugoslav' , 'Guinean' , 'Belorussian' , 'Syrian' , 'Lithuanian' , 'Luxembourger' , 'Angolan' , 'Irishman' , 'Bulgarian' , 'Ugandan' , 'Malawian' , 'American' , 'Hungarian' , 'Argentinian' , 'Icelander' , 'Salvadorean' , 'Afghan' , 'Englishman' , 'Irishman' , 'Kazakh' , 'Madagascan']

temp = [-10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]

year = [2021, 2020, 2019, 2018, 2017,2016,2015,2014,2013,2012,2011,2010,2009,2008,2007,2006,2005,2004,2003,2002,2001,2000]

month = ['January', 'February', 'March', 'April', 'May', 'June','June','August','September','October','November','December']

eyecolor = ['Black', 'Green', 'Blue', 'Light Blue', 'Hazel', 'Amber', 'Brown', 'Gray']

haircolor =['Black', 'White', 'Blond', 'Brown', 'Red', 'Gray']

data = pd.DataFrame()

for i in range(0, 100):
  rcountry = random.choice(country)
  cityindex = country.index(rcountry)
  rcity = random.choice(city.get(cityindex))
  ryear = random.choice(year)
  rday = random.randint(1, 30)
  rdate = datetime.datetime(2021, 9, 1) + datetime.timedelta(days = random.randint(0, 30))
  rmonth = random.choice(month)
  rtemp = random.choice(temp)
  rpop = random.randint(2639858, 86428450)
  rname = random.choice(name)
  rgenre = random.choice(genre)
  rnat = random.choice(nationality)
  rjob = random.choice(job)
  rage = random.randint(0, 100)
  reyec = random.choice(eyecolor)
  rhairc = random.choice(haircolor)
  rheight = round(random.uniform(150.1, 200.0), 1)
  rnr = random.randint(0, 100000)
  rnr2 = random.randrange(-100000, 0)
  rnr3 = random.randrange(-100000, 100000)
  rnr4 = random.random()
  rnr5 = gauss(0,1)
  rnr6 = rand(1)
  rnr7 = random.getrandbits(8)
  rnr8 = random.getrandbits(16)
  rnr9 = random.getrandbits(32)
  alpha = 1
  beta = 1.5
  omega = 3
  rnr10 = random.weibullvariate(alpha, beta)
  rnr11 = round(random.uniform(-999.9, 999.9), 5)
  rnr12 = random.uniform(10.0, 99.9)
  rnr13 = round(random.uniform(-1.0, -99.9), 6)
  rnr14 = round(random.uniform(100.0000, 999.9999), 9)
  rnr15 = random.uniform(-0.0, -1.9)
  rnr16 = round(random.uniform(1.0, 99.9), 1)
  rnr17 = random.uniform(0.0, 1.9)
  rnr18 = round(random.uniform(0.0, 1.9), 8)
  rnr19 = round(random.uniform(-100.0000, -999.9999), 2)
  rnr20 = round(random.uniform(-0.0, -10.9), 4)
  rnr21 = random.uniform(-5.0, 5.0)
  rnr22 = random.randint(0, 1)
  rnr23 = round(random.uniform(-999.9, 999.9), 7)
  rnr24 = round(random.uniform(1.0, 1.9), 1)
  rnr25 = random.paretovariate(omega)
  rnr26 = round(random.uniform(0.0, 10.9), 1)
  rnr27 = random.uniform(-10.0, -99.9)
  rnr28 = random.uniform(-999.9, 999.9)
  rnr29 = round(random.uniform(-0.0, 1.9), 3)
  rnr30 = random.randrange(0, 1001, 2)
  rnr31 = random.randrange(1, 1000, 2)
  


  columns = {'Country': [rcountry], 'City': [rcity], 'Random Year':[ryear], 'Random day':[rday], 'Random Month':[rmonth], 'Random Date':[rdate], 'Random Temperature':[rtemp], 'Random Population':[rpop], 'Random Name':[rname], 'Random Genre':[rgenre], 'Random Nationality':[rnat], 'Random Job':[rjob], 'Random Age':[rage], 'Random Hair Color':rhairc, 'Random Eyes Color':reyec, 'Random Height':rheight, 'Random Number1':[rnr], 'Random Number2':[rnr2], 'Random Number3':[rnr3], 'Random Number4':[rnr4], 'Random Number5':[rnr5], 'Random Number6':[rnr6], 'Random Number7':[rnr7], 'Random Number8':[rnr8], 'Random Number9':[rnr9], 'Random Number10':[rnr10], 'Random Number11':[rnr11], 'Random Number12':[rnr12], 'Random Number13':[rnr13], 'Random Number14':[rnr14], 'Random Number15':[rnr15], 'Random Number16':[rnr16], 'Random Number17':[rnr17], 'Random Number18':[rnr18], 'Random Number19':[rnr19], 'Random Number20':[rnr20], 'Random Number21':[rnr21], 'Random Number22':[rnr22], 'Random Number23':[rnr23], 'Random Number24':[rnr24], 'Random Number25':[rnr25], 'Random Number26':[rnr26], 'Random Number27':[rnr27], 'Random Number28':[rnr28], 'Random Number29':[rnr29], 'Random Number30':[rnr30], 'Random Number31':[rnr31]}
  df = pd.DataFrame(columns)
  data = pd.concat([data, df])
  data.index +=1

data.to_csv (r'X:\Documents\CSV\File Name2.csv')
data.to_json (r'X:\Documents\CSV\File.json')

print(data)