package flink.tablesql;

import java.io.Serializable;

public class Word implements Serializable {
    private String word;
    private int  frequency;

    public Word(String word,int frequency){
        this.word=word;
        this.frequency=frequency;
    }
    public Word(){};

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "Word{" +
                "word='" + word + '\'' +
                ", frequency=" + frequency +
                '}';
    }
}
