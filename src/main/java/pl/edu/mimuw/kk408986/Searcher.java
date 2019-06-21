package pl.edu.mimuw.kk408986;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.uhighlight.DefaultPassageFormatter;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jline.builtins.Completers;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;

public class Searcher {

    private String language = "en";         // en/pl
    private String details = "off";         // on/off
    private int limit = Integer.MAX_VALUE;  // (1...Integer.MAX_VALUE)
    private String color = "off";           // on/off
    private String searchMode = "term";     // term/phrase/fuzzy

    private String bold = "\033[1m";        //ANSI bold code
    private String reset = "\033[0m";       //ANSI reset code

    private static Logger logger = LoggerFactory.getLogger(Searcher.class);

    public static void main (String[] args) {
        Searcher s = new Searcher();
        s.openTerminal();
    }

    private void openTerminal () {
        try {
            Directory indexDir = FSDirectory.open(Paths.get(System.getProperty("user.home") + "/.index"));

            try (Terminal terminal = TerminalBuilder.builder()
                    .nativeSignals(true)
                    .signalHandler(Terminal.SignalHandler.SIG_IGN)
                    .jna(false)
                    .jansi(true)
                    .build()) {
                LineReader lineReader = LineReaderBuilder.builder()
                        .terminal(terminal)
                        .completer(new Completers.FileNameCompleter())
                        .build();

                while (true) {
                    String line = null;
                    try {
                        line = lineReader.readLine("> ");

                        reactOnCommand(line, terminal, indexDir);

                    } catch (UserInterruptException | EndOfFileException e) {
                        break;
                    }
                }
            } catch (IOException e) {
                logger.error("An error has occured", e);
            }
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private void reactOnCommand (String command, Terminal terminal, Directory indexDir) {
            if (command.substring(0, 1).equals("%"))
                settingsCommand(command);
            else
                searchCommand(command, terminal, indexDir);
    }

    private void settingsCommand (String command) {
        if (command.equals("%term"))
            this.searchMode = "term";
        else if (command.equals("%phrase"))
            this.searchMode = "phrase";
        else if (command.equals("%fuzzy"))
            this.searchMode = "fuzzy";
        else if (command.substring(1, "%lang".length()).equals("lang"))
            this.language = command.substring("%lang ".length());
        else if (command.substring(1, "%color".length()).equals("color"))
            this.color = command.substring("%color ".length());
        else if (command.substring(1, "%details".length()).equals("details"))
            this.details = command.substring("%details ".length());
        else if (command.substring(1, "%limit".length()).equals("limit")) {

            if (command.substring("%limit ".length()).equals("0"))
                this.limit = Integer.MAX_VALUE;
            else
                this.limit = Integer.parseInt(command.substring("%limit".length() + 1));
        }
    }

    private void searchCommand (String command, Terminal terminal, Directory indexDir) {
        try {
            IndexReader reader = DirectoryReader.open(indexDir);
            IndexSearcher searcher = new IndexSearcher(reader);
            Analyzer analyzer = createAnalyzer();
            QueryParser parser;
            String field;

            if (language.equals("pl")) {
                parser = new QueryParser("contentspl", analyzer);
                field = "contentspl";
            } else {
                parser = new QueryParser("contentsen", analyzer);
                field = "contentsen";
            }

            Query parsed;

            if (searchMode.equals("term")) {
                TermQuery q = new TermQuery(new Term(field, command));
                parsed = parser.parse(q.toString(field));
            } else if (searchMode.equals("phrase")) {
                String[] wordsTable = command.split(" ");
                PhraseQuery q = new PhraseQuery(field, wordsTable);
                parsed = parser.parse(q.toString(field));
            } else {
                FuzzyQuery q = new FuzzyQuery(new Term(field, command));
                parsed = parser.parse(q.toString(field));
            }

            TopDocs topDocsResult = searcher.search(parsed, limit);
            String[] results = highlightedSearchResults(topDocsResult, searcher, analyzer, parsed);
            displayResults(terminal, searcher, topDocsResult.scoreDocs, results, topDocsResult.totalHits.value);

        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        } catch (ParseException e) {
            logger.error("Parsing exception: ", e);
        }
    }

    private String[] highlightedSearchResults (TopDocs hits, IndexSearcher searcher, Analyzer analyzer, Query query) throws IOException {
        String red = "\033[31m";        //ANSI red color code
        String field = "contentsen";

        UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, analyzer);
        PassageFormatter formatterColor = new DefaultPassageFormatter(red, reset, "...", false);
        PassageFormatter formatterSimple = new DefaultPassageFormatter(bold, reset, "...", false);

        if (color.equals("on"))
            highlighter.setFormatter(formatterColor);
        else
            highlighter.setFormatter(formatterSimple);

        if (language.equals("pl"))
            field = "contentspl";

        return highlighter.highlight(field, query, hits, 5);    //number of maxPasses chosen according to ExampleProject, may be freely changed
    }

    private void displayResults (Terminal terminal, IndexSearcher searcher, ScoreDoc[] scoreDocResults, String[] results, long hits) throws IOException {

        terminal.writer()
                .println(new AttributedStringBuilder().append("Files count: ")
                        .style(AttributedStyle.DEFAULT.bold())
                        .append(Long.toString(hits))
                        .toAnsi());

        for (int i = 0; i < results.length; i++) {

            String name = bold + searcher.doc(scoreDocResults[i].doc).get("path") + reset + "\n";
            terminal.writer()
                    .println(new AttributedStringBuilder()
                            .append(name));

            if (details.equals("on")) {
                terminal.writer()
                        .println(new AttributedStringBuilder()
                                .style(AttributedStyle.DEFAULT.boldDefault())
                                .append(results[i])
                                .toAnsi());
            }
        }
    }

    private Analyzer createAnalyzer () {
        HashMap<String, Analyzer> analyzerMap = new HashMap<>();
        analyzerMap.put("contentspl", new PolishAnalyzer());
        analyzerMap.put("contentsen", new EnglishAnalyzer());

        return new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerMap);
    }

}


