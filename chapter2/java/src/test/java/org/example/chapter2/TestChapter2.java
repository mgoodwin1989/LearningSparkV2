package org.example.chapter2;

//import  org.example.chapter2.JavaWordCount;
import org.junit.jupiter.api.Test;

/**
 * TestChapter2
 */
public class TestChapter2 {

    @Test
    public void testJavaWordCount() {
        JavaWordCount.colorCount("data/mnm_dataset.csv");

    }
}