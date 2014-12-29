package com.youjin;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hello world!
 *
 */
public class App 
{
    private static void createMailFile(String path, String mail_address, String topic, String content) {
        File file = new File(path);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        path = file.getAbsolutePath() + "/" + format.format(new Date()) + ".mail";
        FileWriter fw = null;
        try {
            fw = new FileWriter(path);
            fw.write("to:\"" + mail_address + "\"\n");
            fw.write("topic:\"" + topic + "\"\n");
            fw.write("content:\"" + content + "\"");
            fw.close();
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public static void main( String[] args ) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: send_mail.jar <mail_address> <topic> <content>");
            System.exit(1);
        }
//        String mail_root_path = "/home/wangruo/";

        String mail_root_path = "/data/mail/send_message/";
        String mail_address = args[0];
        String topic = args[1];
        String content = args[2];
        createMailFile(mail_root_path, mail_address, topic, content);
    }
}
