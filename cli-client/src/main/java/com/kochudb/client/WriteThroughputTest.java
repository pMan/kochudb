package com.kochudb.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import com.kochudb.shared.Request;

// https://math.hws.edu/eck/cs225/s10/lab3
public class WriteThroughputTest {

    // 1000 words from https://random-word-api.herokuapp.com/home
    static private String[] words = new String[] { "acknowledgments", "federaleses", "servers", "devolution",
            "numerically", "boarded", "harmful", "liquidators", "redefined", "decolorizes", "muse", "soldans",
            "bluebills", "stewardships", "commonsense", "solions", "backcourts", "tightwires", "maidenhair",
            "technician", "einsteins", "miscreant", "basilects", "crystalloidal", "oodlins", "zeroes", "sovranty",
            "larrikins", "revolutionizing", "metrology", "ut", "clivia", "maut", "nonnasal", "biflagellate",
            "accessorising", "selectivenesses", "dwell", "magnetometry", "cordiality", "lithely", "streamier", "redone",
            "protonation", "myelomas", "sandlotters", "xerophytism", "matrices", "hectographs", "bagwig", "housel",
            "halidome", "aft", "overhandled", "unfallen", "coerects", "telesis", "grimacers", "pockily", "venations",
            "tickseeds", "wren", "reexploring", "teles", "inversed", "filefishes", "assegaied", "isocheim", "sporozoa",
            "cagy", "cultivator", "creep", "bipyramid", "capped", "apraxias", "dimenhydrinate", "saccharase",
            "biofilms", "nondependents", "cogitated", "affordably", "feebleminded", "perceivably", "radiograms",
            "rewaking", "stoniness", "goatskin", "deviled", "cabbalism", "panatella", "demodulated", "telly",
            "prescience", "cochairs", "richen", "stiflers", "atoningly", "oleander", "faintnesses", "toecap", "bowline",
            "copyrights", "barraging", "schooltime", "southwesters", "insufficiencies", "rapaciousnesses",
            "machicolations", "eolipile", "litres", "sunfast", "tubbed", "towardly", "printed", "spacewalking",
            "bitterness", "prejudgers", "whelmed", "hotfooting", "selling", "polemical", "remorselessly", "reimaging",
            "flaggiest", "bidialectal", "cytologists", "isoalloxazine", "collocate", "esterifies", "drat", "scurvy",
            "rootless", "superromantic", "invertase", "kavakavas", "houses", "redraws", "waxweeds", "sextuplicating",
            "instrumentally", "stupendous", "triazoles", "reoccurred", "infested", "sempiternities", "shallot",
            "chatoyances", "payors", "hamulus", "securable", "empathizes", "beclown", "recalculating", "nondisjunction",
            "absolutized", "merchantability", "headstones", "peeresses", "drumbled", "indie", "sibilant", "deckers",
            "preying", "humanizing", "conceptuality", "overlords", "reaccrediting", "waxworks", "thunderbolt",
            "injudicious", "postcaval", "disturbing", "addiction", "organs", "beknotting", "zincking", "exploding",
            "politick", "photogene", "ridings", "dioceses", "houseplant", "resentence", "mothballed", "grosz",
            "reimagining", "aura", "lockram", "snuggle", "pulverizers", "assoiled", "glairiest", "portered",
            "bubalises", "hominian", "paramounts", "felicitates", "supraoptic", "ofter", "strobilus", "orthodox",
            "cortinas", "imponderables", "charcoal", "documentations", "nuptials", "scalawags", "introspected",
            "wholesomest", "antitakeover", "operagoer", "acetylated", "jess", "avatars", "collectanea", "italianize",
            "hijra", "appalling", "illegitimacy", "exodoi", "methylator", "deutzias", "prosodic", "detent", "munchable",
            "saluters", "consolations", "pulchritudes", "agreeable", "adorableness", "rockabyes", "polygons",
            "hierodules", "spahi", "paralegals", "vapid", "ungallantly", "murreys", "alacrities", "seascapes",
            "dishonors", "handstands", "outgaze", "subtropics", "plate", "ameliorator", "aristas", "tenebrionids",
            "layabout", "satisfaction", "primula", "phalansteries", "moan", "suprematisms", "faulds", "fractionalizes",
            "interspaced", "media", "underpainting", "theatric", "gazebos", "frizzed", "where", "reservedness",
            "worriers", "ultrapowerful", "wailers", "epithelializes", "semicolons", "eagerest", "dreamiest", "doumas",
            "animosities", "montage", "photoproducts", "kaons", "offerors", "imbroglio", "clubhand", "spokesperson",
            "dizzinesses", "marauds", "magnetopause", "bistate", "burans", "semiaridity", "exuvial", "aromatherapies",
            "kibbis", "loquacity", "hygienics", "kanji", "subjectivities", "misadd", "inshore", "cradle",
            "polychotomies", "zingiest", "strontian", "transplanters", "sprattle", "piston", "gearwheels", "thesis",
            "abroach", "adjunctly", "hydrolytic", "scalation", "kindhearted", "drawers", "boloneys", "whippletree",
            "coassisting", "interterm", "pyralid", "combos", "trophoblasts", "bleachers", "encounter", "deflexed",
            "maniples", "prepupa", "bronchos", "biennia", "coalyard", "undervalues", "coxcombic", "erasion",
            "medicinals", "cynicism", "compellingly", "unstrings", "ancestor", "hearings", "syringomyelia",
            "disrelished", "oppose", "oreads", "strays", "unapproachably", "anconeal", "cardiological", "obdurateness",
            "banality", "femtoseconds", "precuring", "sussing", "interjoining", "clafouti", "asper", "sutra",
            "recappable", "krill", "recolonized", "nondrinker", "refaces", "loudly", "outsmelling", "phylon",
            "eradicable", "vary", "infusibilities", "synurae", "laminitis", "poetless", "soothers", "polymorphously",
            "isogenous", "names", "bullpen", "downsize", "pitiful", "angered", "emeus", "cheater", "reactionaryisms",
            "eggfruits", "trypsinogens", "bureaucratises", "helpfully", "vitrifiable", "faceup", "nebulose", "pentagon",
            "torsi", "modalities", "romaunts", "collar", "barhopping", "nonfuel", "applause", "sweptwing", "sowans",
            "silvans", "maximizing", "subindex", "shoplifters", "classrooms", "veracious", "ruly", "dazzle", "folding",
            "knobbly", "butanone", "filmography", "storming", "nationally", "errancies", "yachts", "nymphos", "tousing",
            "beautifulness", "centralization", "macks", "payroll", "meshugge", "apophony", "superhot", "punctuators",
            "sopapilla", "melodramatize", "modulatory", "shipworm", "rustled", "explication", "euphorbia", "phut",
            "enjoyment", "tractable", "mildest", "odiousness", "schoolhouse", "autosomes", "frontenises", "vittles",
            "berascaling", "yeggs", "doily", "unbiassed", "underglaze", "resurfacer", "reins", "opine",
            "hyperextensions", "wandered", "bedchair", "flatten", "meagerness", "symbolizations", "resojet", "tinwares",
            "sensually", "repartition", "guylines", "soroche", "haversacks", "infinitesimals", "dishlike", "glistered",
            "pilules", "siliques", "embellisher", "factitive", "persuade", "prestress", "foreordains", "vernissages",
            "estheticism", "mixable", "stochastic", "knapsack", "glossier", "atrophying", "numbered", "subsoiler",
            "haggardly", "axiology", "threeped", "reboarded", "diphtherias", "premaritally", "palaces", "kibitzes",
            "eclipser", "traduces", "sired", "strews", "electrums", "thingamajig", "reticent", "biennium", "hollow",
            "homothallism", "trainbearers", "prosecutors", "poloniums", "interjects", "butterfingers", "handcrafts",
            "embrue", "swimmiest", "ruttishness", "servicers", "dottier", "vinegary", "entoblast", "laryngoscope",
            "unsaved", "chromatinic", "iproniazid", "underpants", "rums", "questionless", "amidols", "chrestomathies",
            "smarminesses", "heptanes", "misprizers", "interferences", "fulvous", "inmates", "kenosises", "spirants",
            "pita", "physiography", "bewilders", "corruptness", "khedival", "geosynchronous", "overbook", "waterpower",
            "supraliminal", "loaning", "granddaughter", "substantivally", "privatdocents", "brisances", "kaross",
            "townish", "cyclazocine", "besoms", "beheading", "womanizing", "clachans", "penetrometers", "albinotic",
            "coremium", "ecotour", "althea", "cabbalahs", "plenteous", "curtainless", "blackbody", "rudenesses",
            "sluggers", "dogcatcher", "futz", "maintains", "timidity", "dependent", "jaunted", "sousaphone",
            "grandioseness", "upbraiding", "carriers", "hummocks", "hostler", "caespitose", "caudal", "specifications",
            "precious", "reerects", "workhour", "xiphisterna", "shrifts", "filoplume", "liard", "destruct", "stolider",
            "wedders", "unlatches", "beclowns", "undercut", "viewiest", "uprootedness", "jerricans", "positives",
            "unsoldierly", "dhows", "veratridines", "shtik", "mas", "attritted", "nonrioter", "ionium", "thaumaturgist",
            "silt", "chronometric", "ossified", "condensible", "mastoidectomy", "taleysim", "airplanes", "psaltries",
            "porphyropsin", "bemurmuring", "depravements", "factoring", "statisticians", "infelicity", "nonfluid",
            "bendays", "divine", "hatefully", "clerkship", "gunshots", "hackler", "culvers", "draftily", "overstocked",
            "ungulates", "curveballed", "ideology", "annals", "outskirt", "befleck", "killings", "jigsawed",
            "jackfruits", "ipecacuanha", "unassumingness", "stenosed", "bijou", "paduasoy", "hedgehopped", "semantical",
            "colicroot", "expellees", "besoothed", "kidney", "participants", "volvuluses", "coassist", "depurating",
            "froglet", "slickest", "demythologized", "nonhomosexuals", "providing", "helminthology", "misdrives",
            "prickles", "goonier", "cellularities", "piccalillis", "gravidities", "impulsive", "headrest", "rowdiness",
            "superconductor", "phoniest", "advowsons", "congregations", "yaud", "monoploid", "crockets", "masturbated",
            "boxwood", "gandered", "cordilleran", "thyrotoxicosis", "gateaux", "turpentines", "nanotubes", "dishwasher",
            "moneywort", "attornments", "quinoas", "workmanly", "gensengs", "finalised", "disposing", "impermanently",
            "patinated", "cocksurely", "byrnies", "vitalizes", "groupers", "ritualism", "quartiers", "tubule",
            "scooching", "saturniid", "powter", "laicize", "munchables", "unhealthinesses", "scrappiest", "screwworm",
            "thermoperiodism", "dorsum", "pillbox", "waggishness", "larges", "sackbuts", "chicks", "infecter",
            "pintles", "subentry", "kaleyards", "sardana", "symptom", "blatantly", "honours", "almoner", "scrummed",
            "quillworts", "hottie", "narcotizes", "aeroscope", "unswathes", "wollastonites", "cyanid", "lamina",
            "litharges", "wino", "quillajas", "tropines", "ectopia", "stifling", "emulators", "incisures", "suppurates",
            "culled", "roaster", "typy", "anomaly", "mezuzas", "dextroses", "ochrea", "libeling", "vaccinated",
            "cavaliering", "rusk", "instroke", "incinerations", "protohistory", "bilections", "metalises", "aurous",
            "pollenate", "fluyt", "endemically", "rhombencephalon", "episodically", "surmised", "saining", "unshifting",
            "hierarchically", "unsaddling", "succours", "whatnesses", "yellowware", "bartend", "diphtheritic",
            "stomatal", "warders", "sharpy", "unquote", "invectivenesses", "boobish", "garbed", "jiggly", "chloridic",
            "rock", "shelta", "antiemetic", "upheavers", "auxiliary", "enterocoeles", "refect", "antismut",
            "untouchability", "encouraged", "cageful", "intendant", "tibias", "mesopauses", "neurasthenic",
            "earthshakers", "reknits", "nonadiabatic", "rato", "spatula", "backhoes", "hatchbacks", "vindicated",
            "pleather", "pawpaw", "pinwale", "oeuvres", "quickening", "subphylum", "dipnoans", "redirecting",
            "calculates", "windsurfs", "bibliophiles", "geohydrologists", "chilies", "conglomerateurs", "parapodium",
            "alliterations", "shamrocks", "siped", "grouch", "internists", "solipsistic", "arrestors", "executers",
            "dichotomous", "protozoa", "uredia", "simious", "reinterview", "hards", "prematurities", "williwaus",
            "constipates", "outhowling", "videocassettes", "evanesce", "tammies", "strongyls", "involucrate",
            "panatelas", "histoid", "segregating", "commencers", "superfamily", "allocated", "rotundnesses",
            "ascribable", "sleave", "emblematizing", "mythicized", "skive", "seeping", "mycetozoans", "mitsvahs",
            "caesarians", "warnings", "orchardist", "synchronize", "squelchier", "tautologically", "materfamilias",
            "soundlessly", "smudgy", "colorism", "inhumes", "sharpshooter", "sudaria", "constructor", "orbited",
            "levitates", "docketed", "telomic", "perigyny", "antiboss", "spotting", "stringybark", "whalelike",
            "decongestants", "trawleys", "egress", "riband", "epidiascope", "crispening", "heartily", "diachronically",
            "impecuniously", "likability", "benni", "desexes", "gospel", "unwarned", "outgenerals", "billionaires",
            "bravenesses", "buffos", "evacuators", "exonerative", "fissioning", "propositioning", "bilateralisms",
            "stirp", "apogeal", "obverting", "subpotent", "zoeal", "bards", "traceried", "your", "neutralizes",
            "shipped", "angeled", "prearranged", "proprietresses", "tabuns", "subtract", "retackling", "flannelling",
            "censoriousness", "violaters", "carpentry", "bobble", "editions", "verbalizations", "stanhope", "mainmast",
            "pancaking", "exhibitor", "carragheen", "cryolites", "satiable", "cystocarp", "nephrectomy", "quags",
            "kishka", "hexapod", "portraiture", "squirmed", "auralities", "carburettor", "willemites", "neighbors",
            "earlier", "predesignating", "beatless", "brut", "saintdoms", "counterevidence", "caroche", "instillments",
            "walkyrie", "cocks", "welfarists", "firebacks", "lovableness", "grieve", "stablest", "toilers",
            "embryogenesis", "locates", "obeahs", "badnesses", "richweed", "enthralls", "anadiplosis", "died", "rabbit",
            "gaurs", "molters", "conceptualises", "lawnmowers", "blinder", "fishhooks", "eiders", "engineerings",
            "fourrageres", "mayvins", "overleapt", "unarched", "voicer", "bureaus", "tattoo", "coonskins", "rapped",
            "comedian", "fellate", "amperes", "incontinent", "sniffish", "clinchers", "muster", "ensouls",
            "overliteral", "buttes", "meterstick", "bloodmobiles", "chanson", "unsnapped", "lactalbumins",
            "overanalyzes", "editrixes", "cutinises", "joyful", "rafting", "sonorants" };

    static private String[] nouns = { "farmer", "rooster", "judge", "man", "maiden", "cow", "dog", "cat", "cheese" };
    static private String[] verbs = { "kept", "waked", "married", "milked", "tossed", "chased", "lay in" };
    static private String[] modifiers = { "that crowed in the morn", "sowing his corn", "all shaven and shorn",
            "all forlorn", "with the crumpled horn" };

    private static Random random = new Random();

    public static String randomWord() {
        return words[random.nextInt(words.length)];
    }

    static String randomNounPhrase() {
        int n = (int) (Math.random() * nouns.length);
        String s = "the " + nouns[n];
        if (Math.random() > 0.75) { // 25% chance of having a modifier.
            int m = (int) (Math.random() * modifiers.length);
            s += " " + modifiers[m];
        }
        int v = (int) (Math.random() * verbs.length);
        s += " that " + verbs[v] + " ";
        if (Math.random() > 0.5) { // 50% chance of having another noun phrase.
            s += randomNounPhrase();
        }
        return s;
    }

    static String randomSimpleSentence() {
        String s = "this is ";
        if (Math.random() > 0.15) { // 85% of sentences have a noun phrase.
            s += randomNounPhrase();
        }
        return s + "the house that Jack built";
    }

    public static String randomSentence() {
        String s = randomSimpleSentence();
        if (Math.random() > 0.75) { // 25% of sentences continue with another clause.
            s += " and " + randomSimpleSentence();
        }
        return s;
    }

    static Map<String, String> createKeyVal(int n) {
        Map<String, String> map = new HashMap<>();
        while (n-- > 0)
            map.put(randomWord(), randomSentence());
        return map;
    }

    public static void insert(Map<String, String> map)
            throws UnknownHostException, IOException, ClassNotFoundException {
        Socket socket = null;

        for (Map.Entry<String, String> e : map.entrySet()) {
            socket = new Socket("localhost", 2222);

            Request dto = new Request("set".getBytes(), e.getKey().getBytes(), e.getValue().getBytes());
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(dto);
            oos.flush();

            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            Request res = (Request) ois.readObject();
            // AssertEquals(res.getData(), "ok");
        }

        if (socket != null)
            socket.close();
    }

    public static void main(String[] args) throws UnknownHostException, ClassNotFoundException, IOException {
        int sets = 100, reps = 100, totalInserts = sets * reps;

        Queue<Map<String, String>> queue = new LinkedList<>();
        while (sets-- > 0) {
            queue.add(createKeyVal(reps));
        }

        sets++;

        long totalTimeInNanos = 0;
        while (!queue.isEmpty()) {
            Map<String, String> map = queue.poll();
            long s = System.nanoTime();
            insert(map);
            long curTimeInNanos = System.nanoTime() - s;
            System.out.println("Time took for set " + sets++ + ": " + (curTimeInNanos / 1_000_000) + "ms");
            totalTimeInNanos += curTimeInNanos;
        }
        System.out.println("total time in ms: " + (totalTimeInNanos / 1_000_000));
        long latency = totalTimeInNanos / totalInserts;
        System.out.println("Average insert time in microseconds: " + (latency / 1000));
    }
}