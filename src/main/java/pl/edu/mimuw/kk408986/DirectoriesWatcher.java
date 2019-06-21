package pl.edu.mimuw.kk408986;

import java.nio.file.*;
import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;
import java.nio.file.attribute.*;
import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.pl.PolishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Index;

public class DirectoriesWatcher {

    private static Logger logger = LoggerFactory.getLogger(DirectoriesWatcher.class);

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;

    @SuppressWarnings("unchecked")
    private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    private void register (Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        keys.put(key, dir);
    }

    private void registerAll (final Path start) throws IOException {
        // register directory and sub-directories
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private DirectoriesWatcher () throws IOException {
        this.watcher = FileSystems.getDefault()
                .newWatchService();
        this.keys = new HashMap<>();
    }

    private void processEvents (FileIndexer indexer, Directory indexDir) {
        for (;;) {
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                logger.warn("WatchKey not recognized!!");
                continue;
            }

            try (IndexWriter writer = createIndexWriter(indexDir)) {
                if (writer == null) {
                    logger.error("Writer could not be created.");
                    continue;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();
                    WatchEvent<Path> ev = cast(event);
                    Path name = ev.context();
                    Path child = dir.resolve(name);

                    if (kind == ENTRY_DELETE || kind == ENTRY_MODIFY) {
                        indexer.deleteDocs(writer, child, kind == ENTRY_MODIFY);
                    }
                    if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                        indexer.indexAllFiles(writer, child, kind == ENTRY_MODIFY);
                    }
                    logger.info("{}: {}", event.kind().name(), child);

                    if (kind == ENTRY_CREATE) {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            addDirectoryToIndexedDirectories(writer, child);
                            registerAll(child);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Low-level I/O error: ", e);
            }

            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);

                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }

    private static void remove (IndexWriter writer, Path path, String index, FileIndexer indexer) {
        ArrayList<String> dirs = indexedDirectories(index);
        boolean exists = false;

        for (String dir : dirs) {
            if (dir.equals(path.toString()))
                exists = true;
        }

        if (exists) {
            indexer.deleteDocs(writer, path, false);
            logger.info("Directory " + path + " removed.");
            return;
        }

        logger.info("Given directory is not indexed.");
    }

    private static void reindex (IndexWriter writer, String index, FileIndexer indexer) {
        try {
            ArrayList<String> indexedDirs = indexedDirectories(index);
            writer.deleteAll();

            if (indexedDirs.isEmpty()) {
                logger.info("No directories to reindex.");
                return;
            }
            for (String dir : indexedDirs) {
                indexer.indexAllFiles(writer, Paths.get(dir), false);
                addDirectoryToIndexedDirectories(writer, Paths.get(dir));
            }
            logger.info("All directories reindexed.");
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static void list (String index) {
        ArrayList<String> indexedDirectories = indexedDirectories(index);

        if (indexedDirectories.isEmpty()) {
            System.out.println("No indexed directories.");
            return;
        }

        System.out.println("Indexed directories: ");

        for (String s : indexedDirectories) {
            System.out.println(s);
        }
    }

    private static void purge (IndexWriter writer) {
        try {
            if (writer == null) {
                logger.error("Index not purged. IndexWriter creating problem.");
                return;
            }
            writer.deleteAll();
            logger.info("Index has been purged.");
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static void add (IndexWriter writer, FileIndexer indexer, Path path) {
        if (!directoryIsIndexed(path)) {
            indexer.indexAllFiles(writer, path, false);
            addDirectoryToIndexedDirectories(writer, path);
        } else {
            System.out.println("Directory is already indexed.");
        }
    }

    private static void addDirectoryToIndexedDirectories (IndexWriter writer, Path dir) {
        try {
            Document d = new Document();
            d.add(new StringField("directory", dir.toString(), Field.Store.YES));
            writer.addDocument(d);
            logger.info("Directory " + dir + " added.");
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static ArrayList<String> indexedDirectories (String index) {
        ArrayList<String> indexedDirs = new ArrayList<>();
        try {
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));

            for (int i = 0; i < reader.maxDoc(); i++) {

                Document doc = reader.document(i);
                String directory = doc.get("directory");

                if (directory != null) {
                    try {
                        indexedDirs.add(directory);
                    } catch (NullPointerException ignore) {
                        logger.error("ArrayList couldn't be created.");
                    }
                }
            }
            reader.close();
        }
        catch (IOException e) {
           logger.error("Low-level I/O error: ", e);
        }
        return indexedDirs;
    }

    private static boolean directoryIsIndexed (Path dir) {
        String index = System.getProperty("user.home") + "/.index";
        return indexedDirectories(index).contains(dir.toString());
    }

    private static void addIndexedDirsToWatcher (String index, DirectoriesWatcher watcher) {
        try {
            ArrayList<String> tabOfDirs = indexedDirectories(index);
            for (String pathString : tabOfDirs) {
                watcher.registerAll(Paths.get(pathString));
            }
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static IndexWriter createIndexWriter (Directory dir) {
        try {
            HashMap<String, Analyzer> analyzerMap = new HashMap<>();
            analyzerMap.put("contentspl", new PolishAnalyzer());
            analyzerMap.put("contentsen", new EnglishAnalyzer());

            Analyzer a = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerMap);
            IndexWriterConfig iwc = new IndexWriterConfig(a);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            return new IndexWriter(dir, iwc);
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
            return null;
        }
    }

    public static void main (String[] args) {
        try {
            Directory indexDirectory = FSDirectory.open(Paths.get(System.getProperty("user.home") + "/.index"));
            String index = System.getProperty("user.home") + "/.index";

            FileIndexer indexer = new FileIndexer(logger);
            DirectoriesWatcher watcher = new DirectoriesWatcher();
            IndexWriter writer = createIndexWriter(indexDirectory);

            if (writer == null) {
                logger.error("IndexWriter not created due to previous error.");
                return;
            }
            if (args.length > 0) {
                switch (args[0]) {
                    case "--purge":
                        purge(writer);
                        break;
                    case "--add":
                        add(writer, indexer, Paths.get(args[1]));
                        break;
                    case "--reindex":
                        reindex(writer, index, indexer);
                        break;
                    case "--list":
                        list(index);
                        break;
                    case "--rm":
                        remove(writer, Paths.get(args[1]), index, indexer);
                        break;
                }
            }
            writer.close();

            if (args.length == 0) {
                logger.info("Indexer has started observation.");
                addIndexedDirsToWatcher(index, watcher);
                watcher.processEvents(indexer, indexDirectory);
            }

        } catch (IOException e) {
            logger.error("Directory does not exists or WatchService problem: ", e);
        }
    }
}