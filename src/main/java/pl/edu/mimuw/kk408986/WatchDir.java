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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.tika.exception.TikaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchDir {

    private static Logger logger = LoggerFactory.getLogger(WatchDir.class);

    private final WatchService watcher;
    private final Map<WatchKey, Path> keys;

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
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

    WatchDir() throws IOException {
        this.watcher = FileSystems.getDefault()
                .newWatchService();
        this.keys = new HashMap<WatchKey, Path>();
    }

    void processEvents(Directory indexDir, FileIndexer indexer, String index) throws ParseException, IOException, TikaException {
        for (; ; ) {
            // wait for key to be signalled
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

                // TBD - provide example of how OVERFLOW event is handled
                if (kind == OVERFLOW) {
                    continue;
                }

                // Context for directory entry event is the file name of entry
                WatchEvent<Path> ev = cast(event);
                Path name = ev.context();
                Path child = dir.resolve(name);

                if (kind == ENTRY_CREATE) {
                    indexer.indexAllFiles(child, indexDir);
                } else if (kind == ENTRY_DELETE) {
                    IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());
                    indexer.deleteFiles(writer, child.toString(), index);
                    writer.close();
                } else if (kind == ENTRY_MODIFY) {
                    IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());
                    indexer.deleteFiles(writer, child.toString(), index);
                    writer.close();
                    indexer.indexAllFiles(child, indexDir);
                }

                // print out event
                logger.info("{}: {}", event.kind().name(), child);

                // if directory is created, and watching recursively, then
                // register it and its sub-directories
                if (kind == ENTRY_CREATE) {
                    try {
                        if (Files.isDirectory(child, NOFOLLOW_LINKS)) {
                            registerAll(child);
                        }
                    } catch (IOException x) {
                        // ignore to keep sample readable
                    }
                }
            }

            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);

                // all directories are inaccessible
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }

    private static Path convertPath(Path path) {
        String pathString = path.toString();
        pathString += "/.index";
        return Paths.get(pathString);
    }

    private static void removeFromIndex(String path, IndexWriter writer, String index, FileIndexer indexer) throws ParseException, IOException {
        String[] dirs = indexedDirectories(index);
        boolean exists = false;
        for (String dir : dirs) {
            if (dir.equals(path))
                exists = true;
        }
        if (exists) {
            indexer.deleteFiles(writer, path, index);
            System.out.println(path);
            Term term = new Term("directory", path);
            writer.deleteDocuments(term);
        }
    }

    private static void reindex(Directory indexDir, String index, FileIndexer indexer)
            throws IOException, ParseException, TikaException {

        String[] indexedDirs = indexedDirectories(index);

        IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());
        writer.deleteAll();
        writer.close();

        if (indexedDirs == null)
            return;
        for (String dir : indexedDirs) {
            indexer.indexAllFiles(Paths.get(dir), indexDir);
        }
    }

    private static void listIndex(String index) throws ParseException, IOException {
        for (String s : indexedDirectories(index)) {
            System.out.println(s);
        }
    }

    private static void purgeIndex (Directory indexDir) throws IOException {
        IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());
        writer.deleteAll();
        writer.close();
    }

    private static void addDirectoryToIndexedDirectories(Path dir, Directory indexDir) throws IOException {

        IndexWriter writer = createIndexWriter(indexDir, new StandardAnalyzer());

        Document d = new Document();
        d.add(new TextField("directory", "dir" + " " + dir.toString(), Field.Store.YES));
        writer.addDocument(d);
        writer.close();
    }

    private static String[] indexedDirectories(String index) throws IOException, ParseException {
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(index)));
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryParser parser = new QueryParser("directory", new StandardAnalyzer());
        Query query = parser.parse("dir");

        ScoreDoc[] result = searcher.search(query, Integer.MAX_VALUE).scoreDocs;

        String[] tabOfDirs = new String[result.length];
        for (int i = 0; i < result.length; i++) {
            Document doc = searcher.doc(result[i].doc);
            tabOfDirs[i] = doc.get("directory").substring("dir ".length());
        }
        return tabOfDirs;
    }

    private static void addIndexedDirsToWatcher(String index, WatchDir watcher) throws IOException, ParseException {
        String[] tabOfDirs = indexedDirectories(index);
        for (String pathString : tabOfDirs) {
            watcher.registerAll(Paths.get(pathString));
        }
    }

    private static IndexWriter createIndexWriter (Directory dir, Analyzer a) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(a);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        return new IndexWriter(dir, iwc);
    }

    public static void main(String[] args) throws IOException, ParseException, TikaException {

        Directory indexDirectory = FSDirectory.open(convertPath(Paths.get(System.getProperty("user.home"))));
        String index = System.getProperty("user.home") + "/.index";

        FileIndexer indexer = new FileIndexer();

        WatchDir watcher = new WatchDir();

        if (args.length > 0) {
            switch (args[0]) {
                case "--purge":
                    purgeIndex(indexDirectory);
                    break;
                case "--add":
                    addDirectoryToIndexedDirectories(Paths.get(args[1]), indexDirectory);
                    indexer.indexAllFiles(Paths.get(args[1]), indexDirectory);
                    break;
                case "--reindex":
                    reindex(indexDirectory, index, indexer);
                    break;
                case "--list":
                    listIndex(index);
                    break;
                case "--rm":
                    IndexWriter writer = createIndexWriter(indexDirectory, new StandardAnalyzer());
                    removeFromIndex(args[1], writer, index, indexer);
                    writer.close();
                    break;
            }
        }


        IndexWriterConfig iwc2 = new IndexWriterConfig(new StandardAnalyzer());
        iwc2.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        IndexWriter writer2 = new IndexWriter(indexDirectory, iwc2);
        if (args.length == 0) {
            addIndexedDirsToWatcher(index, watcher);
            watcher.processEvents(indexDirectory, indexer, index);
        }
        writer2.close();
    }
}