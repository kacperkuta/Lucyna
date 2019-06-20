package pl.edu.mimuw.kk408986;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;

import java.io.File;
import java.io.IOException;

public class MyDocumentParser {

    public static String documentContentToString  (File file) throws IOException, TikaException {
        Tika tika = new Tika();
        return tika.parseToString(file);
    }

    public String documentNameToString (File file) {
        return file.getName();
    }

}
