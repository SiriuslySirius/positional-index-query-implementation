/*
    This program constructs positional indexes 
    and processing phrase and proximity queries
    from a large document corpus (over 30,000 
    documents) from Project Gutenberg. Query 
    results are stored in CSV files for ease of
    reading through spreadsheet software.

    Authors: Abelson Abueg
    CSCI 6030 Information Retrieval Assignment 2
    Date Created: 11 Mar 2022
    Last Updated: 12 Mar 2022
*/

// Java
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

public class PositionalIndex {
    /*
     *
     * CONSTRUCTOR AND CLASS METHODS
     * 
     */

    // TreeMap<Term, DocumentList<DocID, PositionalListing<ListingIndex, DocIndex>>>
    private TreeMap<String, TreeMap<Integer, TreeMap<Integer, Integer>>> positionalIndexData;

    // Default Constructor; it's all you really need.
    public PositionalIndex() {
        this.positionalIndexData = new TreeMap<String, TreeMap<Integer, TreeMap<Integer, Integer>>>();
    }

    public void updatePositionalIndex(String term, int docID, int docPosition) {
        // If the term exists in the Positional Index Data
        if (this.positionalIndexData.containsKey(term)) {
            // If the document exists in the Positional Index Data
            if (this.positionalIndexData.get(term).containsKey(docID)) {
                this.positionalIndexData.get(term).get(docID).put(
                        /*
                         * size will be the size before insertion providing 0-based indexing
                         * for the key in the positional listing.
                         */
                        this.positionalIndexData.get(term).get(docID).size(), docPosition);
            } else {
                // Create new Positional List
                TreeMap<Integer, Integer> newPositionalList = new TreeMap<Integer, Integer>();
                // Insert new position into the new Positional List
                newPositionalList.put(newPositionalList.size(), docPosition);
                // Insert new document into the Document List
                this.positionalIndexData.get(term).put(docID, newPositionalList);
            }
        } else {
            // Create new Positional List
            TreeMap<Integer, Integer> newPositionalList = new TreeMap<Integer, Integer>();
            // Insert new position into the new Positional List
            newPositionalList.put(newPositionalList.size(), docPosition);
            // Create a new Document List
            TreeMap<Integer, TreeMap<Integer, Integer>> newDocumentList = new TreeMap<Integer, TreeMap<Integer, Integer>>();
            // Insert new Document with Positional List into Document List
            newDocumentList.put(docID, newPositionalList);
            // Insert new Document List into the Term TreeMap
            this.positionalIndexData.put(term, newDocumentList);
        }
    }

    int getTermCount() {
        return this.positionalIndexData.size();
    }

    int getDocumentCountByTerm(String term) {
        return this.positionalIndexData.get(term).size();
    }

    int getPositionalCountByTermAndDocID(String term, int docID) {
        return this.positionalIndexData.get(term).get(docID).size();
    }

    // Print the stats of the PositionalIndex object.
    void printStats() {
        System.out.println("Number of Terms: " + this.getTermCount());
        Set<Map.Entry<String, TreeMap<Integer, TreeMap<Integer, Integer>>>> termEntry = this.positionalIndexData
                .entrySet();
        termEntry.forEach(term -> {
            Set<Map.Entry<Integer, TreeMap<Integer, Integer>>> docEntry = term.getValue().entrySet();
            System.out.println("\t" + term.getKey() + ": " + term.getValue().size());
            docEntry.forEach(doc -> {
                System.out.println("\t\t" + doc.getKey() + ": " + doc.getValue().size());
            });
        });
    }

    // Searches for phrases between two queried words within k words and print
    // result into CSV file. Handles both directions.
    void proximitySearch(String x, String y, int k) {
        // Case where term doesn't exist.
        if (this.positionalIndexData.get(x) != null && this.positionalIndexData.get(y) != null) {
            // Create a String ArrayList for storing the result and have it sorted.
            ArrayList<String> xFirstResults = new ArrayList<String>();
            ArrayList<String> yFirstResults = new ArrayList<String>();

            // Get the Document List from the Terms
            TreeMap<Integer, TreeMap<Integer, Integer>> xDocList = this.positionalIndexData.get(x);
            TreeMap<Integer, TreeMap<Integer, Integer>> yDocList = this.positionalIndexData.get(y);

            // For iterating at the DocList level.
            Set<Map.Entry<Integer, TreeMap<Integer, Integer>>> xEntry = xDocList.entrySet();
            Set<Map.Entry<Integer, TreeMap<Integer, Integer>>> yEntry = yDocList.entrySet();

            // Handle x...y
            xEntry.forEach(doc -> {
                if (yDocList.get(doc.getKey()) != null) {
                    // For iterating at Positional Listing level.
                    Set<Map.Entry<Integer, Integer>> xPositionEntry = doc.getValue().entrySet();
                    Set<Map.Entry<Integer, Integer>> yPositionEntry = yDocList.get(doc.getKey()).entrySet();
                    xPositionEntry.forEach(xPosition -> {
                        yPositionEntry.forEach(yPosition -> {
                            if (xPosition.getValue() + (k) == yPosition.getValue()) {
                                xFirstResults.add(
                                        doc.getKey() + "," + xPosition.getValue() + "," + yPosition.getValue());
                            }
                        });
                    });
                }
            });

            // Handle y...x
            yEntry.forEach(doc -> {
                if (xDocList.get(doc.getKey()) != null) {
                    // For iterating at Positional Listing level.
                    Set<Map.Entry<Integer, Integer>> yPositionEntry = doc.getValue().entrySet();
                    Set<Map.Entry<Integer, Integer>> xPositionEntry = xDocList.get(doc.getKey()).entrySet();
                    yPositionEntry.forEach(yPosition -> {
                        xPositionEntry.forEach(xPosition -> {
                            if (yPosition.getValue() + (k) == xPosition.getValue()) {
                                yFirstResults.add(
                                        doc.getKey() + "," + yPosition.getValue() + "," + xPosition.getValue());
                            }
                        });
                    });
                }
            });

            // Skip if ArrayLists are empty
            if (!xFirstResults.isEmpty() || !yFirstResults.isEmpty()) {
                System.out.println("Writing results into CSV files...");

                PrintWriter resultWriter = null;
                PrintWriter detailedWriter = null;
                try {
                    resultWriter = new PrintWriter(outputPath + "\\" + x + "_" + y + "_" + k + "_" + outputFiles[0],
                            "UTF-8");
                    detailedWriter = new PrintWriter(outputPath + "\\" + x + "_" + y + "_" + k + "_" + outputFiles[1],
                            "UTF-8");
                } catch (IOException ex) {
                    System.err.println(ex);
                    System.err.println("Program terminated\n");
                    System.exit(1);
                }

                // Print CSV Headers
                resultWriter.println("DocID,First Position,Second Position");
                detailedWriter.println("DocID,Filepath,First Position,Second Position,Exact Phrase");

                // Handle x...y
                for (String triple : xFirstResults) {
                    resultWriter.println(triple);
                    String[] splitTriple = triple.split(",");
                    String phrase = "";
                    int position = 0;

                    // Get exact phrase
                    try {
                        Pattern wordPattern = Pattern.compile("[a-zA-Z]+");
                        Matcher wordMatcher;
                        String line, word = "";
                        BufferedReader br = new BufferedReader(
                                new FileReader(inputFileNames.get(Integer.parseInt(splitTriple[0]) - 1)));

                        while ((line = br.readLine()) != null && position < Integer.parseInt(splitTriple[2])) {
                            // process the line by extracting words using the wordPattern
                            wordMatcher = wordPattern.matcher(line);

                            // Process one word at a time
                            while (wordMatcher.find()) {
                                position++;
                                // Extract and convert the word to lowercase
                                word = line.substring(wordMatcher.start(), wordMatcher.end());
                                if (position >= Integer.parseInt(splitTriple[1])
                                        && position <= Integer.parseInt(splitTriple[2])) {
                                    phrase = phrase + " " + word;
                                }

                            } // while - wordMatcher
                        } // while - Line
                    } catch (IOException ex) {
                        System.err.println("File " + inputFileNames.get(Integer.parseInt(splitTriple[0]) - 1)
                                + " not found. Program terminated.\n");
                        System.exit(1);
                    }

                    detailedWriter.println(splitTriple[0] + "," +
                            inputFileNames.get(Integer.parseInt(splitTriple[0]) - 1) + "," +
                            splitTriple[1] + "," +
                            splitTriple[2] + "," +
                            phrase.trim());
                }

                // Handle y...x
                for (String triple : yFirstResults) {
                    resultWriter.println(triple);
                    String[] splitTriple = triple.split(",");
                    String phrase = "";
                    int position = 0;

                    // Get exact phrase
                    try {
                        Pattern wordPattern = Pattern.compile("[a-zA-Z]+");
                        Matcher wordMatcher;
                        String line, word = "";
                        BufferedReader br = new BufferedReader(
                                new FileReader(inputFileNames.get(Integer.parseInt(splitTriple[0]) - 1)));

                        while ((line = br.readLine()) != null && position < Integer.parseInt(splitTriple[2])) {
                            // process the line by extracting words using the wordPattern
                            wordMatcher = wordPattern.matcher(line);

                            // Process one word at a time
                            while (wordMatcher.find()) {
                                position++;
                                // Extract and convert the word to lowercase
                                word = line.substring(wordMatcher.start(), wordMatcher.end());
                                if (position >= Integer.parseInt(splitTriple[1])
                                        && position <= Integer.parseInt(splitTriple[2])) {
                                    phrase = phrase + " " + word;
                                }

                            } // while - wordMatcher
                        } // while - Line
                    } catch (IOException ex) {
                        System.err.println("File " + inputFileNames.get(Integer.parseInt(splitTriple[0]) - 1)
                                + " not found. Program terminated.\n");
                        System.exit(1);
                    }

                    detailedWriter.println(splitTriple[0] + "," +
                            inputFileNames.get(Integer.parseInt(splitTriple[0]) - 1) + "," +
                            splitTriple[1] + "," +
                            splitTriple[2] + "," +
                            phrase.trim());
                }
                resultWriter.close();
                detailedWriter.close();

            } else {
                System.out.println("No results found from your query.");
            }
        } else {
            System.out.println("No results found from your query.");
        }
    }

    /*
     *
     * GLOBAL VARIABLES
     *
     */
    // An array to hold Gutenberg corpus file names
    static ArrayList<String> inputFileNames = new ArrayList<String>();

    // To keep count of the amount of files in the corpus provided
    static int fileCount = 0;

    // An array of default output file names for ease of access.
    static String outputFiles[] = new String[] {
            "proximity_query_result.csv",
            "proximity_query_detailed_result.csv",
    };

    static String outputPath = "";

    /*
     *
     * HELPER METHODS
     *
     */

    /*
     * loads all files names in the directory subtree into an array
     * violates good programming practice by accessing a global variable
     * (inputFileNames)
     */
    public static void listFilesInPath(final File path) {
        for (final File fileEntry : path.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesInPath(fileEntry);
            } else if (fileEntry.getName().endsWith((".txt"))) {
                fileCount++;
                inputFileNames.add(fileEntry.getPath());
            }
        }
    }

    /*
     *
     * MAIN METHOD
     *
     */
    public static void main(String[] args) {
        long startTime = System.nanoTime();
        int error = 0;

        /*
         * Did the user provide correct number of command line arguments?
         * If not, print message and exit
         */
        if (args.length != 5) {
            System.err.println("\nNumber of command line arguments must be 5");
            System.err.println("You have given " + args.length + " command line arguments");
            System.err.println("Incorrect usage. Program terminated");
            System.err.println(
                    "Correct usage: java PositionalIndex <path-to-input-files> <path-to-out-result-files> <first-word> <second-word> <int-distance-between-words>");
            error = 1;
        }
        if (!(args[2] != null && args[2].matches("^[a-zA-Z]*$"))) {
            System.err.println("Error: <first-word> argument must only have alphabet letters in the input.");
            error = 1;
        }
        if (!(args[3] != null && args[3].matches("^[a-zA-Z]*$"))) {
            System.err.println("Error: <second-word> argument must only have alphabet letters in the input.");
            error = 1;
        }
        if (Integer.parseInt(args[4]) < 1) {
            System.err.println("Error: <int-distance-between-words> argument must be greater than 0.");
            error = 1;
        }
        if (error == 1) {
            System.exit(1);
        }

        /*
         * If the files exists, we need to empty them.
         * We will be appending new data into the documents
         */
        for (String file : outputFiles) {
            String path = args[1] + "\\" + args[2].toLowerCase() + "_" + args[3].toLowerCase() + "_" + args[4] + "_"
                    + file;
            try {
                new PrintWriter(path, "UTF-8").close();
            } catch (FileNotFoundException ex) {
                System.err.println(ex);
                System.err.println("\nProgram terminated\n");
                System.exit(1);
            } catch (UnsupportedEncodingException ex) {
                System.err.println(ex);
                System.err.println("\nProgram terminated\n");
                System.exit(1);
            }
        }

        /*
         * Extract input file name from command line arguments
         * This is the name of the file from the Gutenberg corpus
         */
        String inputFileDirName = args[0];
        System.out.println("\nInput files directory path name is: " + inputFileDirName);

        /*
         * Extract output path from command line arguments
         * This is the name of the file from the Gutenberg corpus
         */
        outputPath = args[1];
        System.out.println("Output directory path name is: " + outputPath);

        // Collects file names and write them to
        listFilesInPath(new File(inputFileDirName));
        System.out.println("Number of Gutenberg corpus files: " + fileCount);

        // br for efficiently reading characters from an input stream
        BufferedReader br = null;

        /*
         * wordPattern specifies pattern for words using a regular expression
         * wordMatcher finds words by spotting word patterns with input
         */
        Pattern wordPattern = Pattern.compile("[a-zA-Z]+");
        Matcher wordMatcher;

        /*
         * line - a line read from file
         * word - an extracted word from a line
         */
        String line, word;

        // Initialize new Positional Index
        PositionalIndex positionalIndex = new PositionalIndex();

        System.out.println("\nBuilding Positional Index...");

        // Process one file at a time
        for (int index = 0; index < fileCount; index++) {
            System.out.println("Processing: " + inputFileNames.get(index));

            // Keep track of document position.
            int docPosition = 0;

            /*
             * Keep track of Doc ID for assignment for building the positional
             * index data. They start at 1.
             */
            int docID = 1 + index;

            /*
             * Open the input file, read one line at a time, extract words
             * in the line, extract characters in a word, write words and
             * character counts to disk files
             */
            try {
                /*
                 * Get a BufferedReader object, which encapsulates
                 * access to a (disk) file
                 */
                br = new BufferedReader(new FileReader(inputFileNames.get(index)));

                /*
                 * As long as we have more lines to process, read a line
                 * the following line is doing two things: makes an assignment
                 * and serves as a boolean expression for while test
                 */
                while ((line = br.readLine()) != null) {
                    // process the line by extracting words using the wordPattern
                    wordMatcher = wordPattern.matcher(line);

                    // Will store a cleaner version of line into String ArrayList
                    ArrayList<String> cleanLine = new ArrayList<String>();

                    // Process one word at a time
                    while (wordMatcher.find()) {

                        // Extract and convert the word to lowercase
                        word = line.substring(wordMatcher.start(), wordMatcher.end());
                        cleanLine.add(word.toLowerCase());
                    } // while - wordMatcher

                    /*
                     * Handles cases if the line is empty
                     *
                     * Without this, it will count empty strings
                     * because cleanLine is originally empty.
                     */
                    if (!cleanLine.isEmpty()) {
                        for (String term : cleanLine) {
                            positionalIndex.updatePositionalIndex(term, docID, ++docPosition);
                        }
                    }
                } // while - Line
            } // try
            catch (IOException ex) {
                System.err.println("File " + inputFileNames.get(index) + " not found. Program terminated.\n");
                System.exit(1);
            }
        } // for -- Process one file at a time

        System.out.println("\nPositional Index Built.");
        System.out.println("\nNow performing proximity search...");
        positionalIndex.proximitySearch(args[2].toLowerCase(), args[3].toLowerCase(), Integer.parseInt(args[4]));

        // End Process Timer
        long endTime = System.nanoTime();
        System.out.println("\nProcess Completed in " +
                (double) (endTime - startTime) / 1_000_000_000 + " seconds.\n");
    } // main()
} // class
