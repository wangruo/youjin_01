package com.youjin.tools.lac;

import com.google.gson.Gson;

import java.io.*;

/**
 * Hello world!
 *
 */
public class App 
{


    public static class LacData{
        private String name;
        private String[] geoCoord;

        public LacData(String name, String x, String y)
        {
            this.name = name;
            geoCoord = new String[]{x,y};
        }
    }

    public static void main( String[] args ) throws IOException {

        if(args.length != 2){
            System.out.println("Usages: lac <inputFile> <outputFile>");
            return;
        }

        Gson gson = new Gson();
        FileWriter writer = new FileWriter(args[1]);

        FileInputStream inputStream = new FileInputStream(args[0]);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] str = line.split("\t");
            if(str.length != 9)
            {
                System.out.println("line error, length:" + str.length);
                continue;
            }
            LacData lacData = new LacData(str[2]+"-"+str[3], str[5], str[6]);
            String json = gson.toJson(lacData, LacData.class);

            writer.write(json + ",\r\n");
        }
        writer.close();
        reader.close();
    }
}
