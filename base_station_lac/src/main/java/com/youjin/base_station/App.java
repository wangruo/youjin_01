package com.youjin.base_station;

import java.io.*;


public class App
{
    public static void main( String[] args ) throws IOException {

        if(args.length != 2){
            System.out.println("Usages: lac <inputFile> <outputFile>");
            return;
        }

        FileWriter writer = new FileWriter(args[1]);

        FileInputStream inputStream = new FileInputStream(args[0]);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] str = line.split("\t");
            if(str.length != 23)
            {
                System.out.println("line error, length:" + str.length);
                continue;
            }

            writer.write(str[8] + "," + str[7] + "\r\n");
        }
        writer.close();
        reader.close();
    }
}
