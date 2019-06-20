package pl.edu.mimuw.kk408986;

import java.awt.datatransfer.SystemFlavorMap;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageResult;
import ucar.ma2.Index;


public class FileIndexer {

    public void indexFile (Path file, Directory indexDir) throws IOException, TikaException {

        String fileConversion = MyDocumentParser.documentContentToString(new File(file.toString()));
        Document doc = new Document();
        IndexWriter writer;

        if (langDetector(fileConversion).equals("pl"))
            writer = createIndexWriter(indexDir, new PolishAnalyzer());
        else
            writer = createIndexWriter(indexDir, new EnglishAnalyzer());

        Field pathField = new TextField("path", file.toString(), Field.Store.YES);
        doc.add(pathField);

        Field nameField = new TextField("name", file.getFileName().toString(), Field.Store.YES);
        doc.add(nameField);

        doc.add(new TextField("contents", fileConversion, Field.Store.YES));

        if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
            System.out.println("adding " + file);
            writer.addDocument(doc);
        } else {
            System.out.println("updating " + file);
            writer.updateDocument(new Term("path", file.toString()), doc);
        }

        writer.close();
    }


    public void indexAllFiles (Path path, Directory indexDir) throws IOException, TikaException {
        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile (Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        indexFile(file, indexDir);
                    } catch (IOException | TikaException ignore) {
                        // don't index files that can't be read or parsed.
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            indexFile(path, indexDir);
        }
    }

    public void UpdateFilesUnderDirectory (Path dir, String index, Directory indexDir) throws ParseException, IOException {
        IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());
        deleteFiles(writer, dir.toString(), index);
        try {
            indexAllFiles(dir, indexDir);
        } catch (IOException | TikaException ignore) {
            // don't index files that can't be read or parsed.
        }
    }

    //Przerobic String dir na Path dir!!!
    public void deleteFiles (final IndexWriter writer, String dir, String index) throws ParseException, IOException {

        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));

        for (int i = 0; i < reader.numDocs(); i++) {

            if (reader.document(i).get("path") != null &&
                    reader.document(i).get("path").substring(0, dir.length()).equals(dir)) {

                writer.deleteDocuments(new Term("path", reader.document(i).get("path")));

            }
        }
        reader.close();
    }

    private String langDetector (String s) {
        OptimaizeLangDetector detector = new OptimaizeLangDetector();
        detector.loadModels();
        detector.addText(s.toCharArray(), 0, s.length());
        List<LanguageResult> l = detector.detectAll();

        return l.get(0).getLanguage();
    }

    private static IndexWriter createIndexWriter (Directory dir, Analyzer a) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(a);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        return new IndexWriter(dir, iwc);
    }
}
