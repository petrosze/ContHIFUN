package utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class ReadPathsFromLocalFile {

    // Windows local path
	private static final String lfile = "C:\\path\\to\\local\\paths.txt"; 
   	 
    // ISL local node path
    //private static final String lfile = "/paths/to/hdfs/paths.txt";
	
	public ArrayList<String> getPaths() throws FileNotFoundException {
		ArrayList<String> list = new ArrayList<String>();
		File file = new File(lfile);
	    Scanner reader = new Scanner(file);
	    while (reader.hasNextLine()) {
	    	String path = reader.nextLine();
	        list.add(path);
	    }
	    reader.close();
	    return list;
	}
}
