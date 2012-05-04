/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package net.flighttweets.tweets;

/**
 *
 * @author imathew
 */
public class FlightKeyword {
    int id;
    String word;
    public void setId(int newId) {
        id = newId;
    }
    public int getId() {
        return id;
    }
    public void setWord(String newWord) {
        word = newWord;
    }
    public String getWord() {
        return word;
    }
}
