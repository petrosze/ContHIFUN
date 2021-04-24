package utils;

import java.util.Queue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StreamSimulator {

	private String dir_path;
	private JavaStreamingContext ssc;

	public StreamSimulator(String folder_path, JavaStreamingContext ssc) {
		this.dir_path = folder_path;
		this.ssc = ssc;
	}

	public Queue<JavaRDD<String>> getQueueOfRDDs() throws IOException {

		List<String> filePaths = new ArrayList<String>();
		FileSystem fs = null;
		Path path = new Path(dir_path);
		fs = path.getFileSystem(new Configuration());
		path = fs.resolvePath(path);

		if (fs.isDirectory(path)) {
			FileStatus[] fileStatuses = fs.listStatus(path);
			for (FileStatus fileStatus : fileStatuses) {
				if (fileStatus.isFile())
					filePaths.add(fileStatus.getPath().toString());
			}
		} else {
			filePaths.add(path.toString());
		}

		Queue<JavaRDD<String>> rddsQueue = new LinkedList<>();
		for (int index = 0; index < filePaths.size(); index++) {
			JavaRDD<String> rdd = ssc.sparkContext().textFile(filePaths.get(index));
			rddsQueue.add(rdd);
		}
		return rddsQueue;
	}
}
