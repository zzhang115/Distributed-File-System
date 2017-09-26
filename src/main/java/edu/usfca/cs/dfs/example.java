package edu.usfca.cs.dfs;
import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;
/**
 * Created by zzc on 9/26/17.
 */
public class example {

        public static List<String> words = new ArrayList<String>();
        public static Map<String, Integer> wordMap = new HashMap<String, Integer>();

        public static void washWord() {
            String temp;
            for (int i = 0; i < words.size(); i++) {
                temp = words.get(i);
                temp = temp.replaceAll("[^a-zA-Z]", "");
                words.set(i, temp);
            }
        }

        public static void countWord() {
            for (String word : words) {
                word = word.toLowerCase();
                if (wordMap.keySet().contains(word)) {
                    int value = wordMap.get(word);
                    wordMap.put(word, ++value);
                } else {
                    wordMap.put(word, 1);
                }
            }
        }
        private static class wordComparator implements Comparator<Map.Entry<String, Integer>> {
            public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
                if (a.getValue() > b.getValue()) {
                    return -1;
                } else if (a.getValue() < b.getValue()) {
                    return 1;
                } else {
                    String stra = a.getKey();
                    String strb = b.getKey();
                    int i = 0;
                    while (i < stra.length() && i < strb.length()) {
                        if (stra.charAt(i) < strb.charAt(i)) {
                            return -1;
                        } else if (stra.charAt(i) > strb.charAt(i)) {
                            return 1;
                        }
                        i++;
                    }
                    if (i == stra.length()) {
                        return -1;
                    } else if (i == strb.length()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            }
        }

        public static void main(String args[] ) throws Exception {
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.nextLine();
                for (String word : line.split("\\s+")) {
                    words.add(word);
                }
            }
            washWord();
            countWord();
            List<Map.Entry<String, Integer>> entryList = new LinkedList<Map.Entry<String, Integer>>(wordMap.entrySet());
            Collections.sort(entryList, new wordComparator());
            int nums = entryList.size() > 10 ? 10 : entryList.size();
            for (int i = 0; i < nums; i++) {
                System.out.println(entryList.get(i).getKey() + " " + entryList.get(i).getValue());
            }
        }
}
