package com.weblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import com.weblog.logstat.LogFileProtosParquet.CommonLogParquet;

import parquet.hadoop.metadata.CompressionCodecName;
import parquet.proto.ProtoParquetOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class WashLogParquet {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text txtContent = new Text();
		private HashSet<String> hashSetIp = new HashSet<String>();
		private HashSet<String> hashSetUa = new HashSet<String>();
		private HashMap<String, String> hashInfoIP = new HashMap<String, String>();
		private HashMap<String, String> hashInfoUA = new HashMap<String, String>();
				
		protected void cleanup(Context context) throws IOException, InterruptedException {

			// System.out.println(Map.hashSetIp.size());
			int ipNum = hashSetIp.size();			
			int i = 0;
			int sliceNum = 10000;
			StringBuilder sb = new StringBuilder();
			Iterator<String> iterator = hashSetIp.iterator();
			while (iterator.hasNext()) {
				String item = iterator.next();
				sb.append(item + ",");

				if (i % sliceNum == 0 && i != 0) {
					String ipstr = sb.toString();
					ipstr = ipstr.substring(0, ipstr.length() - 1);
					String ipjson =  WashUtils.getJsonIP(ipstr);
//					System.out.println(ipstr);
//					System.out.println(ipjson);
					JSONArray jsonArray = new JSONArray(ipjson);
					for (int j = 0; j < jsonArray.length(); j++) {
						if ( !jsonArray.get(j).equals(null) ) {
							JSONObject jsonObject = (JSONObject) jsonArray.get(j);
							String key = jsonObject.get("ip").toString();
//							if (!hashInfoIP.containsKey(key))
								hashInfoIP.put(key, jsonObject.get("json").toString());
						}
					}
					sb = new StringBuilder();
					System.out.println("HashSetIP\t"+ipNum);
					System.out.println("================="+i+"=================");
				} else if (i + 1 == ipNum) {
					String ipstr = sb.toString();
					ipstr = ipstr.substring(0, ipstr.length() - 1);
					String ipjson =  WashUtils.getJsonIP(ipstr);
//					System.out.println(ipstr);
//					System.out.println(ipjson);
					JSONArray jsonArray = new JSONArray(ipjson);
					for (int j = 0; j < jsonArray.length(); j++) {
						if ( !jsonArray.get(j).equals(null) ) {
							JSONObject jsonObject = (JSONObject) jsonArray.get(j);
							String key = jsonObject.get("ip").toString();
//							if (!hashInfoIP.containsKey(key))
								hashInfoIP.put(key, jsonObject.get("json").toString());
						}
					}
					sb = new StringBuilder();
				}

				i++;
			}

			// System.out.println(Map.hashSetUa.size());
			int uaNum = hashSetUa.size();
			i = 0;
			sb = new StringBuilder();
			iterator = hashSetUa.iterator();
			while (iterator.hasNext()) {

				String item = iterator.next();
				sb.append(item + "^^");

				if (i % sliceNum == 0 && i != 0) {
					String uastr = sb.toString();
					uastr = uastr.substring(0, uastr.length() - 2);
					String uajson = WashUtils.getJsonUA(uastr);
//					System.out.println(uastr);
//					System.out.println(uajson);
					JSONArray jsonArray = new JSONArray(uajson);
					for (int j = 0; j < jsonArray.length(); j++) {
						if ( !jsonArray.get(j).equals(null) ) {
							JSONObject jsonObject = (JSONObject) jsonArray.get(j);
							String key =  WashUtils.md5(jsonObject.get("ua").toString());
							if (!hashInfoUA.containsKey(key))
								hashInfoUA.put(key, jsonObject.get("json").toString());
						}
					}
					sb = new StringBuilder();
					System.out.println("HashSetUA\t"+uaNum);
					System.out.println("================="+i+"=================");
				} else if (i + 1 == uaNum) {
					String uastr = sb.toString();
					uastr = uastr.substring(0, uastr.length() - 2);
					String uajson =  WashUtils.getJsonUA(uastr);
//					System.out.println(uastr);
//					System.out.println(uajson);
					JSONArray jsonArray = new JSONArray(uajson);
					for (int j = 0; j < jsonArray.length(); j++) {
						if ( !jsonArray.get(j).equals(null) ) {
							JSONObject jsonObject = (JSONObject) jsonArray.get(j);
							String key =  WashUtils.md5(jsonObject.get("ua").toString());
							if (!hashInfoUA.containsKey(key))
								hashInfoUA.put(key, jsonObject.get("json").toString());
						}
					}
					sb = new StringBuilder();
				}

				i++;
			}		
			
			
			try {
                Path hashInfoIpPath = new Path("hdfs://cdh180:8020/user/yarn/hashInfoIP");
                Path hashInfoUaPath = new Path("hdfs://cdh180:8020/user/yarn/hashInfoUA");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                ObjectOutputStream oos = new ObjectOutputStream(fs.create(hashInfoIpPath, true));                
                oos.writeObject(hashInfoIP);
                oos.close();
                oos = new ObjectOutputStream(fs.create(hashInfoUaPath, true));
                oos.writeObject(hashInfoUA);
                oos.close();
	        } catch(Exception ex) {
	            ex.printStackTrace();
	        }
						
			hashSetIp = null;
			hashSetUa = null;
			hashInfoIP = null;
			hashInfoUA = null;
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String pattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]*)\"";

			String line = value.toString();
			String ip = "";
			String ts = "";
			// String date = "";
			// String time = "";
			String qs = "";
			String ho = "";
			String ho_hf = "";
			String id = "";
			String sid = "";
			String ua = "";
			String rf = "";
			String hf = "";
			// String output_ip = "";
			// String output_ua = "";
			String dt = "";
			String src = "";
			URL uri_qs = null;

			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(line);
			if (m.find()) {
				ip = m.group(1);
				ts = m.group(4);
				r = Pattern.compile("GET (/stat.gif.*) HTTP/.*");
				Matcher m2 = r.matcher(m.group(5));
				String req  = "";
				if (m2.find()) {
					req = m2.group(1);
				} else {
					return;
				}
				qs = req.replace("\\x", "\\\\x");
				rf = m.group(8);
				if (rf.equals("-"))
					rf = "";
				ua = m.group(9);
				// System.out.println("IP Address: " + m.group(1));
				// System.out.println("Date&Time: " + m.group(4));
				// System.out.println("Request: " + m.group(5));
				// System.out.println("Response: " + m.group(6));
				// System.out.println("Bytes Sent: " + m.group(7));
				// if (!m.group(8).equals("-"))
				// System.out.println("Referer: " + m.group(8));
				// System.out.println("Browser: " + m.group(9));
			} else {
				System.out.println(value.toString());
				System.out.println("NO MATCH");
				return;
			}

			try {
//				if (!qs.startsWith("/stat.gif"))
//					return;
				uri_qs = new URL("http://localhost" + qs);
				HashMap<String, String> hm =  WashUtils.splitQuery(uri_qs);
				String str_qa = new JSONObject(hm).toString();
				if (hm.containsKey("ho"))
					ho = hm.get("ho");
				if (hm.containsKey("id"))
					id = hm.get("id");
				if (hm.containsKey("sid"))
					sid = hm.get("sid");
				else
					sid = id;
				if (hm.containsKey("ip"))
					ip = hm.get("ip");
				if (hm.containsKey("ua")) {
					ua = hm.get("ua");
					ua = URLDecoder.decode(ua, "utf-8");
				}
				if (hm.containsKey("rf")) {
					rf = hm.get("rf").toLowerCase();
					rf = URLDecoder.decode(rf, "utf-8");
				}
				if (hm.containsKey("hf")) {
					hf = hm.get("hf");
					hf = URLDecoder.decode(hf, "utf-8");
					if (!hf.equals("") && !hf.equals("-") && hf.toLowerCase().startsWith("http://")) {
						uri_qs = new URL(hf);
						ho_hf = uri_qs.getHost();
						hf = uri_qs.getPath();
						if (uri_qs.getQuery() != null)
							hf += "?" + uri_qs.getQuery();
					}
				}

				if (!id.equals("")) {
					SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
					SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					try {
						Date d = df.parse(ts);
						dt = df2.format(d);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					hashSetIp.add(ip);
					hashSetUa.add(ua);

					String tmp_qa = "";
					try{
						tmp_qa = str_qa;
						str_qa = str_qa.replaceAll("%", "%25");
						str_qa = str_qa.replaceAll("%25([A-Fa-f0-9]{2})", "%$1");
						str_qa = str_qa.replaceAll("%5C([^%22]?)", "%5C%5C$1");
						str_qa = str_qa.replaceAll("([^%5C]?)%22", "$1%5C%22");
						str_qa = str_qa.replaceAll("%09", "%20%20%20%20");
						str_qa = URLDecoder.decode(str_qa, "utf-8");
					}catch(Exception e){
						e.printStackTrace();
						System.out.println(tmp_qa);
						System.out.println(str_qa);
						return;
					}
					
					if (rf.startsWith("http://") && !(rf.startsWith("http://"+ho) || rf.startsWith("https://"+ho)))
						src = new URL(rf).getHost();					
					if (ho.equals(""))
						ho = ho_hf;
					String content = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", ho, id, sid, ip, dt, src, ua, rf, hf, str_qa);					
					word.set(ho + "\t" + sid);
					txtContent.set(content);
					context.write(word, txtContent);
				}
			} catch (MalformedURLException e) {
				e.printStackTrace();
			}
		}
	}

	public static class Combine extends Reducer<Text,Text,Text,Text> {  
		private Text word = new Text();
		private Text txtContent = new Text();
		
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
            context.write(word, txtContent);  
        }
    }
	
	public static class Reduce extends Reducer<Text, Text, Void, CommonLogParquet> {
		BytesWritable output = new BytesWritable();

		private HashMap<String, String> hashInfoIP = new HashMap<String, String>();
		private HashMap<String, String> hashInfoUA = new HashMap<String, String>();
		
//		private HashSet<String> hashSetIp = new HashSet<String>();
//		private HashSet<String> hashSetUa = new HashSet<String>();
		
		@SuppressWarnings("unchecked")
		protected void setup(Context context) throws IOException, InterruptedException {
			// Configuration conf = context.getConfiguration();

			try {
                Path hashInfoIpPath = new Path("hdfs://cdh180:8020/user/yarn/hashInfoIP");
                Path hashInfoUaPath = new Path("hdfs://cdh180:8020/user/yarn/hashInfoUA");
                FileSystem fs = FileSystem.get(context.getConfiguration());
                ObjectInputStream ois = new ObjectInputStream(fs.open(hashInfoIpPath));
                hashInfoIP = (HashMap<String, String>) ois.readObject();
                ois.close();
                ois = new ObjectInputStream(fs.open(hashInfoUaPath));
                hashInfoUA = (HashMap<String, String>) ois.readObject();
                ois.close();
	        } catch(Exception ex) {
	            ex.printStackTrace();
	        }
			

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			hashInfoIP = null;
			hashInfoUA = null;
//			hashSetIp = null;
//			hashSetUa = null;
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();
			ArrayList<String> al_na = new ArrayList<String>();
			ArrayList<String> al_mo = new ArrayList<String>();
			ArrayList<String> al_pr = new ArrayList<String>();
			ArrayList<String> al_ci = new ArrayList<String>();
			ArrayList<String> al_ip = new ArrayList<String>();
			ArrayList<String> al_ad = new ArrayList<String>();
			ArrayList<String> al_hw = new ArrayList<String>();
			ArrayList<String> al_ua = new ArrayList<String>();
			HashMap<String, String> hm_vp = new HashMap<String, String>();
			String domain = "";
			String id = "";
			String sid = "";
			String ip = "";
			String datetime = "";
			String ua = "";
			String ipjson = "";
			String devicejson = "";
			String ds = "";
			String rf = "";
			String hf = "";
			String src = "";
			int n = 0;
			List<String> sortedList = new ArrayList<>();
			for (Text value : values)
			    sortedList.add(value.toString());
			Collections.sort(sortedList, new Comparator<String>() {
				@Override
				public int compare(String a, String b) {
					SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					try {
						return Long.compare(
								fmt.parse(a.split("\t")[4]).getTime(), 
								fmt.parse(b.split("\t")[4]).getTime()
								);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						System.exit(-1);
					}
					return -1;
				}
			});
			for (String value : sortedList) {
				String[] words = value.split("\t");
				domain = words[0];
				id = words[1];
				sid = words[2];
				ip = words[3];
				datetime = words[4];
				ds = datetime.split(" ")[0];
				if (src.equals(""))
					src = words[5];
				ua = words[6];
				rf = words[7];
				hf = words[8];
				if (hf.equals("") && (n+1) < sortedList.size()) {
					hf = sortedList.get(n+1).split("\t")[7];
					if (!hf.equals("") && !hf.equals("-") && hf.toLowerCase().startsWith("http://")){
						URL uri_qs = new URL(hf);
						hf = uri_qs.getPath();
						if (uri_qs.getQuery() != null)
							hf += "?" + uri_qs.getQuery();
					}
				}
				hm_vp.put(datetime, hf);

				String userinfo = words[words.length - 1];
				JSONObject jsonObject = new JSONObject();
				try{
					jsonObject = new JSONObject(userinfo);
				}catch(Exception e){
					System.out.println("==================================================");
					System.out.println(userinfo);
					e.printStackTrace();
				}
				
				String na = "";
				String mo = "";
				String pr = "";
				String ci = "";
				String _ip = ip;
				String ad = "";
				String hw = "";

				if (jsonObject.has("na")) 
					na = jsonObject.getString("na");
				if (jsonObject.has("mo"))
					mo = jsonObject.getString("mo");
				if (jsonObject.has("pr")) 
					pr = jsonObject.getString("pr");
				if (jsonObject.has("ci"))
					ci = jsonObject.getString("ci");
				if (jsonObject.has("ip"))
					_ip = jsonObject.getString("ip");
				if (jsonObject.has("ad"))
					ad = jsonObject.getString("ad");
				if (jsonObject.has("hw"))
					hw = jsonObject.getString("hw");
				if (jsonObject.has("ua"))
					ua = jsonObject.getString("ua");

				if (!al_na.contains(na) && !na.equals("")) {
					al_na.add(na);
				}
				if (!al_mo.contains(mo) && !mo.equals("") &&  WashUtils.isNumeric(mo)) {
					al_mo.add(mo);
				}
				if (!al_pr.contains(pr) && !pr.equals("")) {
					al_pr.add(pr);
				}
				if (!al_ci.contains(ci) && !ci.equals("")) {
					al_ci.add(ci);
				}
				if (!al_ip.contains(_ip) && !_ip.equals("")) {
					al_ip.add(_ip);
				}
				if (!al_ad.contains(ad) && !ad.equals("")) {
					al_ad.add(ad);
				}
				if (!al_hw.contains(hw) && !hw.equals("")) {
					al_hw.add(hw);
				}
				if (!al_ua.contains(ua) && !ua.equals("")) {
					al_ua.add(ua);
				}
				n++;
			}
			
			map.put("na", al_na);
			map.put("mo", al_mo);
			map.put("pr", al_pr);
			map.put("ci", al_ci);
			map.put("ip", al_ip);
			map.put("ad", al_ad);
			map.put("hw", al_hw);
			map.put("ua", al_ua);
			String info = new JSONObject(map).toString();

			// String content = String.format("%s\t%s\t%s\t%s\t%s\t%s", domain,
			// id, ip, datetime, device, info);
			// System.out.println(content);

			CommonLogParquet.Builder comlog = CommonLogParquet.newBuilder();
			comlog.setId(id);
			comlog.setSid(sid);
			comlog.setDomain(domain);
			comlog.setIp(ip);
			comlog.setReferer(rf);
			comlog.setPv(n);
			comlog.setSource(src);

			if (!ip.equals("")) {
				if (hashInfoIP.containsKey(ip)) {
					ipjson = hashInfoIP.get(ip);
				}
			}

			if (!ua.equals("")) {
				String ua_md5 =  WashUtils.md5(ua);
				if (hashInfoUA.containsKey(ua_md5)) {
					devicejson = hashInfoUA.get(ua_md5);
				}
			}

			// System.out.println(ipjson);
			CommonLogParquet.IPInfo.Builder ipinfo = CommonLogParquet.IPInfo.newBuilder();
			if (!ipjson.equals("")) {
				JSONObject jsonObject = new JSONObject(ipjson);
				if (jsonObject.has("country"))
					ipinfo.setCountry(jsonObject.getString("country"));
				if (jsonObject.has("province"))
					ipinfo.setProvince(jsonObject.getString("province"));
				if (jsonObject.has("city"))
					ipinfo.setCity(jsonObject.getString("city"));
				if (jsonObject.has("isp"))
					ipinfo.setIsp(jsonObject.getString("isp"));
			}

			CommonLogParquet.UAInfo.Builder uainfo = CommonLogParquet.UAInfo.newBuilder();
			if (!devicejson.equals("")) {
				JSONObject jsonObject = new JSONObject(devicejson);
				if (jsonObject.has("Device"))
					uainfo.setDevice(jsonObject.getString("Device"));
				if (jsonObject.has("Os"))
					uainfo.setOs(jsonObject.getString("Os"));
				if (jsonObject.has("Browser"))
					uainfo.setBrowser(jsonObject.getString("Browser"));
				if (jsonObject.has("Engine"))
					uainfo.setEngine(jsonObject.getString("Engine"));
				if (jsonObject.has("Nettype"))
					uainfo.setNettype(jsonObject.getString("Nettype"));
				if (jsonObject.has("Language"))
					uainfo.setLanguage(jsonObject.getString("Language"));
			}
				
			ArrayList<String> al_vp = new ArrayList<String>();
			List<String> keys = new ArrayList<String>(hm_vp.keySet());
			Collections.sort(keys);
			String stayTime = String.format("%02d:%02d:%02d", 0, 0, 0);
			SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Long prevVisitTime = new Long(0);
			for (int i = 0; i < keys.size(); i++) {
				String dt = keys.get(i);
				String href = hm_vp.get(dt);
				
				if (prevVisitTime > 0) {
					try {
						Long totalSecs = (fmt.parse(dt).getTime() - prevVisitTime)/1000;
						Long hours = totalSecs / 3600;
						Long minutes = (totalSecs % 3600) / 60;
						Long seconds = totalSecs % 60;

						stayTime = String.format("%02d:%02d:%02d", hours, minutes, seconds);
					} catch (ParseException e1) {
						System.err.println(dt);
						e1.printStackTrace();
					}
				} else {
					datetime = dt;
				}
				
				al_vp.add(href+"\t"+stayTime+"\t"+dt);
				
				try {
					prevVisitTime = fmt.parse(dt).getTime();
				} catch (ParseException e1) {
					System.err.println(dt);
					e1.printStackTrace();
				}
//				System.out.println(keys.get(i));
			}
//			System.out.println("");
			CommonLogParquet.PathInfo.Builder pathinfo = CommonLogParquet.PathInfo.newBuilder();
			for (int i = 0; i < al_vp.size(); i++) {
				CommonLogParquet.UrlInfo.Builder urlinfo = CommonLogParquet.UrlInfo.newBuilder();
				String vp = al_vp.get(i);
				String[] arrVp = vp.split("\t");
				urlinfo.setUrl(arrVp[1]);
			    urlinfo.setStaytime(arrVp[0]);
			    urlinfo.setVisittime(arrVp[2]);
			    pathinfo.addUrlinfo(i, urlinfo.build());
			}

			comlog.setDatetime(datetime);
			comlog.setIpinfo(ipinfo.build());
			comlog.setUainfo(uainfo.build());
			comlog.setUserinfo(info);
			comlog.setPathinfo(pathinfo.build());

//			context.write(key, new BytesWritable(comlog.build().toByteArray()));
			context.write(null, comlog.build());
		}
	}

	public static void execMRTask(Configuration configuration, Path pathInput, Path pathOutput) throws Exception {

		Job job = new Job(configuration, "Wash Log"+pathOutput.getName());

		job.setJarByClass(WashLogParquet.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(CommonLogParquet.class);       
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		
		job.setOutputFormatClass(ProtoParquetOutputFormat.class);
        ProtoParquetOutputFormat.setProtobufClass(job, CommonLogParquet.class);
        
        ProtoParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
		
		FileInputFormat.addInputPath(job, pathInput);
		FileOutputFormat.setOutputPath(job, pathOutput);

		if (job.waitForCompletion(true)) {
			HdfsUtils.execHive("msck repair table logstat_daily");
			HdfsUtils.execImpala("refresh logstat_daily");
		}
	}
	
	public static void main(String[] args) throws Exception {		
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://cdh180:8020");
		configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		configuration.set("io.seqfile.compression.type", "BLOCK");
		configuration.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		configuration.set("mapred.compress.map.output", "true");
		// configuration.set("mapred.map.output.compression.codec",
		// "com.hadoop.compression.lzo.LzoCodec");
		configuration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		// configuration.set("io.compression.codecs","org.apache.hadoop.io.compress.SnappyCodec");	
		configuration.set("io.compression.codecs",
				"org.apache.hadoop.io.compress.DefaultCodec," + "com.hadoop.compression.lzo.LzopCodec,"
						+ "org.apache.hadoop.io.compress.SnappyCodec," + "org.apache.hadoop.io.compress.GzipCodec");
		
		FileSystem fs = FileSystem.get(configuration);
		String statlogs = "E:\\works\\mapred_stats\\statlogs";
		if (args.length > 1)
			statlogs = args[1];
//		File dir = new File("/root/statlogs");
		File dir = new File(statlogs);
		Path pathInput = new Path("/logs/tmp_input/");
		File[] fList = dir.listFiles();
		if (fList != null) {
			Arrays.sort(fList, new Comparator<File>() {
				@Override
				public int compare(File a, File b) {
					return Long.compare(a.lastModified(), b.lastModified());
				}
			});
			for (File child : fList) {
				if (!fs.exists(pathInput))
					fs.mkdirs(pathInput);
				System.out.println(child.getAbsolutePath());
				fs.copyFromLocalFile(new Path(child.getAbsolutePath()), pathInput);
				String[] slice = child.getName().split("_");
				slice = slice[1].split("\\.");
				String date = slice[0];
				Path pathOutput = new Path("/user/hive/warehouse/logstat_daily/ds=" + date);
				if (fs.exists(pathOutput))
					fs.delete(pathOutput);
				execMRTask(configuration, pathInput, pathOutput);
				fs.delete(pathInput);
			}
		}
		fs.close();
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}