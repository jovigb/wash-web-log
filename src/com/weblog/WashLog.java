package com.weblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import com.hadoop.compression.lzo.LzoCodec;
import com.hadoop.compression.lzo.LzopCodec;
import com.weblog.logstat.LogFileProtos.CommonLog;

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
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class WashLog {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text txtContent = new Text();
		private static HashSet<String> hashSetIp = new HashSet<String>();
		private static HashSet<String> hashSetUa = new HashSet<String>();

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
				hf = m.group(8);
				if (!hf.equals("") && !hf.equals("-")) {
					uri_qs = new URL(hf);
					ho_hf = uri_qs.getHost();
					hf = uri_qs.getPath();
					if (uri_qs.getQuery() != null)
						hf += "?" + uri_qs.getQuery();
				}
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
					rf = hm.get("rf");
					rf = URLDecoder.decode(rf, "utf-8");
				}
				if (hm.containsKey("hf")) {
					hf = hm.get("hf");
					hf = URLDecoder.decode(hf, "utf-8");
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
					if (ho.equals(""))
						ho = ho_hf;
					String content = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", ho, id, sid, ip, dt, ua, rf, hf, str_qa);					
//					if (id.equals("aoddo8sm8cigm2lot2sol2f890")) {
//						System.out.println(content);
//						System.out.println(qs);
//						System.out.println(m.group(5));
//					}
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
	
	public static class Reduce extends Reducer<Text, Text, Text, BytesWritable> {
		BytesWritable output = new BytesWritable();

		private HashMap<String, String> hashInfoIP = new HashMap<String, String>();
		private HashMap<String, String> hashInfoUA = new HashMap<String, String>();

		protected void setup(Context context) throws IOException, InterruptedException {
			// Configuration conf = context.getConfiguration();

			// System.out.println(Map.hashSetIp.size());
			int ipNum = Map.hashSetIp.size();
			int i = 0;
			int sliceNum = 10000;
			StringBuilder sb = new StringBuilder();
			Iterator<String> iterator = Map.hashSetIp.iterator();
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
			int uaNum = Map.hashSetUa.size();
			i = 0;
			sb = new StringBuilder();
			iterator = Map.hashSetUa.iterator();
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

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			hashInfoIP = null;
			hashInfoUA = null;
//			Map.hashSetIp = null;
//			Map.hashSetUa = null;
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
			int n = 0;
			for (Text value : values) {
				String[] words = value.toString().split("\t");
				domain = words[0];
				id = words[1];
				sid = words[2];
				ip = words[3];
				datetime = words[4];
				ds = datetime.split(" ")[0];
				ua = words[5];
				if (n == 0)
					rf = words[6];
				hf = words[7];
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

//				if (id.equals("aoddo8sm8cigm2lot2sol2f890")) {
//					System.out.println(value);
//				}
				n++;
			}
//			if (id.equals("aoddo8sm8cigm2lot2sol2f890")) {
//				System.out.println(ua);
//			 	System.exit(0);
//			}
			
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

			CommonLog.Builder comlog = CommonLog.newBuilder();
			comlog.setId(id);
			comlog.setSid(sid);
			comlog.setDomain(domain);
			comlog.setIp(ip);
			comlog.setReferer(rf);

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
			CommonLog.IPInfo.Builder ipinfo = CommonLog.IPInfo.newBuilder();
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

			CommonLog.UAInfo.Builder uainfo = CommonLog.UAInfo.newBuilder();
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

			CommonLog.UserInfo.Builder userinfo = CommonLog.UserInfo.newBuilder();
			if (!info.equals("")) {
				JSONObject jsonObject = new JSONObject(info);
				if (jsonObject.has("ip")) {
					JSONArray jsonList = jsonObject.getJSONArray("ip");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addIp(jsonList.getString(i));
					}
				}
				if (jsonObject.has("na")) {
					JSONArray jsonList = jsonObject.getJSONArray("na");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addName(jsonList.getString(i));
					}
				}
				if (jsonObject.has("ge")) {
					JSONArray jsonList = jsonObject.getJSONArray("ge");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addGender(jsonList.getString(i));
					}
				}
				if (jsonObject.has("pr")) {
					JSONArray jsonList = jsonObject.getJSONArray("pr");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addProvince(jsonList.getString(i));
					}
				}
				if (jsonObject.has("ci")) {
					JSONArray jsonList = jsonObject.getJSONArray("ci");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addCity(jsonList.getString(i));
					}
				}
				if (jsonObject.has("di")) {
					JSONArray jsonList = jsonObject.getJSONArray("di");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addDistrict(jsonList.getString(i));
					}
				}
				if (jsonObject.has("mo")) {
					JSONArray jsonList = jsonObject.getJSONArray("mo");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addMobile(jsonList.getString(i));
					}
				}
				if (jsonObject.has("ad")) {
					JSONArray jsonList = jsonObject.getJSONArray("ad");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.addAddress(jsonList.getString(i));
					}
				}
				if (jsonObject.has("hw")) {
					JSONArray jsonList = jsonObject.getJSONArray("hw");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.setHw(jsonList.getString(i));
					}
				}
				if (jsonObject.has("ua")) {
					JSONArray jsonList = jsonObject.getJSONArray("ua");
					for (int i = 0; i < jsonList.length(); i++) {
						userinfo.setUa(jsonList.getString(i));
					}
				}
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
			CommonLog.PathInfo.Builder pathinfo = CommonLog.PathInfo.newBuilder();			
			for (int i = 0; i < al_vp.size(); i++) {
				CommonLog.UrlInfo.Builder urlinfo = CommonLog.UrlInfo.newBuilder();
				String vp = al_vp.get(i);
				String[] arrVp = vp.split("\t");
				urlinfo.setUrl(arrVp[0]);
			    urlinfo.setStaytime(arrVp[1]);
			    urlinfo.setVisittime(arrVp[2]);
			    pathinfo.addUrlinfo(urlinfo);
			}


			comlog.setDatetime(datetime);
			comlog.setIpinfo(ipinfo);
			comlog.setUainfo(uainfo);
			comlog.setUserinfo(userinfo);
			comlog.setPathinfo(pathinfo);

			context.write(key, new BytesWritable(comlog.build().toByteArray()));
		}
	}

	public static void execMRTask(Configuration configuration, Path pathInput, Path pathOutput) throws Exception {
//		Date currentTime = new Date();
//		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
//		int MILLIS_IN_DAY = 1000 * 60 * 60 * 24;
//		String prevDate = formatter.format(currentTime.getTime() - MILLIS_IN_DAY);

		Job job = new Job(configuration, "Wash Log"+pathOutput.getName());

//		String input = "/user/root/log_test/";
		// String input = "/user/root/input_gz/";
		// String input = "/user/root/input_lzo/";
//		String output = "/user/hive/warehouse/logstat/dd=" + date;

		// String input = args[0];
		// String output = args[1];

//		deleteDir(configuration, pathOutput);

		job.setJarByClass(WashLog.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		// job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, true);
		// SequenceFileOutputFormat.setOutputCompressorClass(job,
		// LzoCodec.class);
		SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		FileInputFormat.addInputPath(job, pathInput);
		FileOutputFormat.setOutputPath(job, pathOutput);
		// FileInputFormat.addInputPath(job, new Path(input));
		// FileOutputFormat.setOutputPath(job, new Path(output));

		// Submit the job to the cluster and wait for it to finish.
		if (job.waitForCompletion(true)) {
			HdfsUtils.execHive("msck repair table logstat");
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
		String statlogs = "E:\\works\\wash-web-log\\statlogs";
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
				Path pathOutput = new Path("/user/hive/warehouse/logstat/dd=" + date);
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