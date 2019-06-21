package pl.edu.mimuw.kk408986;

import java.nio.file.*;
import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;
import java.nio.file.attribute.*;
import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoriesWatcher {

    private static Logger logger = LoggerFactory.getLogger(DirectoriesWatcher.class);

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;

    @SuppressWarnings("unchecked")
    private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        keys.put(key, dir);
    }

    private void registerAll(final Path start) throws IOException {
        // register directory and sub-directories
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private DirectoriesWatcher() throws IOException {
        this.watcher = FileSystems.getDefault()
                .newWatchService();
        this.keys = new HashMap<>();
    }

    private void processEvents(Directory indexDir, FileIndexer indexer) {
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

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);

                if (kind == ENTRY_DELETE || kind == ENTRY_MODIFY) {
                    indexer.deleteDocs(child, indexDir, kind == ENTRY_MODIFY);
                }
                if (kind == ENTRY_CREATE || kind == ENTRY_MODIFY) {
                    indexer.indexAllFiles(child, indexDir, kind == ENTRY_MODIFY);
                }

                logger.info("{}: {}", event.kind().name(), child);

                if (kind == ENTRY_CREATE) {
                    try {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            addDirectoryToIndexedDirectories(child, indexDir);
                            registerAll(child);
                        }
                    } catch (IOException e) {
                        logger.error("Low-level I/O error: ", e);
                    }
                }
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

    private static void removeFromIndex(Path path, Directory indexDir, String index, FileIndexer indexer) {
        ArrayList<String> dirs = indexedDirectories(index);
        boolean exists = false;

        for (String dir : dirs) {
            if (dir.equals(path.toString()))
                exists = true;
        }

        if (exists) {
            indexer.deleteDocs(path, indexDir, false);
            logger.info("Directory " + path + " removed.");
            return;
        }

        logger.info("Given directory is not indexed.");
    }

    private static void reindex(Directory indexDir, String index, FileIndexer indexer) {
        try {
            ArrayList<String> indexedDirs = indexedDirectories(index);

            IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());
            if (writer != null) {
                writer.deleteAll();
                writer.close();
            } else {
                //if writer is null error was described to logger.
                return;
            }

            if (indexedDirs.isEmpty()) {
                logger.info("No directories to reindex.");
                return;
            }
            for (String dir : indexedDirs) {
                indexer.indexAllFiles(Paths.get(dir), indexDir, false);
                addDirectoryToIndexedDirectories(Paths.get(dir), indexDir);
            }
            logger.info("All directories reindexed.");
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static void listIndex(String index) {
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

    private static void purgeIndex (Directory indexDir) {
        try {
            IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());
            if (writer == null) {
                logger.error("Index not purged. IndexWriter creating problem.");
                return;
            }
            writer.deleteAll();
            writer.close();
            logger.info("Index has been purged.");
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static void addDirectoryToIndexedDirectories(Path dir, Directory indexDir) {
        try {
            IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());

            if (writer != null) {           //if writer is null Exception was described on logger
                Document d = new Document();
                d.add(new StringField("directory", dir.toString(), Field.Store.YES));
                writer.addDocument(d);
                writer.close();
                logger.info("Directory " + dir + " added.");
            }
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static ArrayList<String> indexedDirectories(String index) {

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
                        //directory is not null
                    }
                }
            }
            reader.close();
        }
        catch (IOException e) {
           //
        }
        return indexedDirs;
    }

    private static void addIndexedDirsToWatcher(String index, DirectoriesWatcher watcher) {
        try {
            ArrayList<String> tabOfDirs = indexedDirectories(index);
            for (String pathString : tabOfDirs) {
                watcher.registerAll(Paths.get(pathString));
            }
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
        }
    }

    private static IndexWriter createIndexWriter (Directory dir, Analyzer a) {
        try {
            IndexWriterConfig iwc = new IndexWriterConfig(a);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            return new IndexWriter(dir, iwc);
        } catch (IOException e) {
            logger.error("Low-level I/O error: ", e);
            return null;
        }
    }

    public static void main(String[] args) {
        try {
            Directory indexDirectory = FSDirectory.open(Paths.get(System.getProperty("user.home") + "/.index"));
            String index = System.getProperty("user.home") + "/.index";

            FileIndexer indexer = new FileIndexer(logger);

            DirectoriesWatcher watcher = new DirectoriesWatcher();

            if (args.length > 0) {
                switch (args[0]) {
                    case "--purge":
                        purgeIndex(indexDirectory);
                        break;
                    case "--add":
                        indexer.indexAllFiles(Paths.get(args[1]), indexDirectory, false);
                        addDirectoryToIndexedDirectories(Paths.get(args[1]), indexDirectory);
                        break;
                    case "--reindex":
                        reindex(indexDirectory, index, indexer);
                        break;
                    case "--list":
                        listIndex(index);
                        break;
                    case "--rm":
                        removeFromIndex(Paths.get(args[1]), indexDirectory, index, indexer);
                        break;
                }
            }
            if (args.length == 0) {
                logger.info("Indexer has started observation.");
                addIndexedDirsToWatcher(index, watcher);
                watcher.processEvents(indexDirectory, indexer);
            }
        } catch (IOException e) {
            logger.error("Directory does not exists or WatchService problem: ", e);
        }
    }
}