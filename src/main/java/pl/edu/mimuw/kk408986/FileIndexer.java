package pl.edu.mimuw.kk408986;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.slf4j.Logger;


public class FileIndexer {

    private Logger logger;

    public FileIndexer (Logger logger) {
        this.logger = logger;
    }

    private String documentContentToString  (File file) throws IOException, TikaException {
        Tika tika = new Tika();
        return tika.parseToString(file);
    }

    private String langDetector (String s) {
        OptimaizeLangDetector detector = new OptimaizeLangDetector();
        detector.loadModels();
        detector.addText(s.toCharArray(), 0, s.length());
        List<LanguageResult> l = detector.detectAll();

        return l.get(0).getLanguage();
    }

    private void indexFile (IndexWriter writer, Path file, boolean updating) throws IOException, TikaException {

        String fileConversion = documentContentToString(new File(file.toString()));
        Document doc = new Document();

        doc.add(new StringField("name", file.getFileName().toString(), Field.Store.YES));

        doc.add(new StringField("path", file.toString(), Field.Store.YES));

        if (langDetector(fileConversion).equals("pl"))
            doc.add(new TextField("contentspl", fileConversion, Field.Store.YES));
        else
            doc.add(new TextField("contentsen", fileConversion, Field.Store.YES));

        if (!updating) {
            System.out.println("adding ... " + file);
            writer.addDocument(doc);
        } else {
            System.out.println("updating ... " + file);
            writer.updateDocument(new Term("path", file.toString()), doc);
        }
    }

    public void indexAllFiles (IndexWriter writer, Path path, boolean updating) {
        try {
            if (Files.isDirectory(path)) {
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        try {
                            indexFile(writer, file, updating);
                        } catch (IOException | TikaException ignore) {
                            //don't index files, that can't be read or parsed.
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
            } else {
                indexFile(writer, path, updating);
            }
        } catch (TikaException | IOException ignore) {
            //don't index files, that can't be read or parsed.
        }
    }

    public void deleteDocs (IndexWriter writer, Path delPath, boolean updating) {
        String deletedPath = delPath.toString();

        try {
            String index = System.getProperty("user.home") + "/.index";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));

            for (int i = 0; i < reader.maxDoc(); i++) {

                Document doc = reader.document(i);
                String path = doc.get("path");
                String directory = doc.get("directory");

                if (path != null && path.indexOf(deletedPath) == 0) {
                    writer.deleteDocuments(new Term("path", path));
                    if (!updating)
                        System.out.println("deleting ... " + path);
                }
                System.out.println("dir: " + directory);
                if (directory != null && directory.equals(deletedPath)) {
                    writer.deleteDocuments(new Term("directory", deletedPath));
                }
            }
            reader.close();
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }
}
