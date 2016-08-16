package com.weblog;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.InflaterInputStream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class WashUtils {

	protected WashUtils() {}
	
	public static byte[] gzip(byte[] input) throws Exception {
        GZIPOutputStream gzipOS = null;
        try {
            System.out.println("before gzip:"+input.length);
            ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
            gzipOS = new GZIPOutputStream(byteArrayOS) {
                {
                    this.def.setLevel(Deflater.BEST_COMPRESSION);
                }
            };
            gzipOS.write(input);
            gzipOS.flush();
            gzipOS.close();
            gzipOS = null;
            System.out.println("after gzip:"+byteArrayOS.toByteArray().length);
            return byteArrayOS.toByteArray();
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (gzipOS != null) {
                try { gzipOS.close(); } catch (Exception ignored) {}
            }
        }
    }
    
    public static byte[] deflate(byte[] input) throws Exception {
    	DeflaterOutputStream defOS = null;
        try {
            System.out.println("before deflate:"+input.length);
            ByteArrayOutputStream byteArrayOS = new ByteArrayOutputStream();
            defOS = new DeflaterOutputStream(byteArrayOS);
            defOS.write(input);
            defOS.flush();
            defOS.close();
            defOS = null;
            System.out.println("after deflate:"+byteArrayOS.toByteArray().length);
            return byteArrayOS.toByteArray();
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (defOS != null) {
                try { defOS.close(); } catch (Exception ignored) {}
            }
        }
    }
    
    public static byte[] gunzip(byte[] input) throws Exception {
        GZIPInputStream gis = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
        	ByteArrayInputStream bis = new ByteArrayInputStream(input);
            gis =new GZIPInputStream(bis);
            int len = -1;
            byte [] b1 =new byte[1024];
//            StringBuilder sb = new StringBuilder();
            while((len = gis.read(b1)) != -1){
                bos.write(b1, 0, len);
            }
            bos.close();
            bis.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new Exception(e);
        } finally {
            if (gis != null) {
                try { gis.close(); } catch (Exception ignored) {}
            }
        }
    }
    
	public static String sendPost(String url, String param) {
		PrintWriter out = null;
		BufferedReader in = null;
		String result = "";
		try {
			URL realUrl = new URL(url);
			// 打开和URL之间的连接
			URLConnection conn = realUrl.openConnection();
			// 设置通用的请求属性
			conn.setRequestProperty("accept", "*/*");
//			conn.setRequestProperty("connection", "Keep-Alive");
//			conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			conn.setRequestProperty("Content-Encoding", "deflate");
			conn.setRequestProperty("Transfer-Encoding", "chunked");
			// 发送POST请求必须设置如下两行
			conn.setDoOutput(true);
			conn.setDoInput(true);
			conn.getOutputStream().write(deflate(param.getBytes()));
//			// 获取URLConnection对象对应的输出流
//			out = new PrintWriter(conn.getOutputStream());
//			// 发送请求参数
//			out.print(param);
//			// flush输出流的缓冲
//			out.flush();
			// 定义BufferedReader输入流来读取URL的响应
			String encoding = conn.getContentEncoding();
			if (encoding.equals("gzip")) {
				in = new BufferedReader(new InputStreamReader(new GZIPInputStream(conn.getInputStream())));
			} else if (encoding.equals("deflate")) {
				in = new BufferedReader(new InputStreamReader(new InflaterInputStream(conn.getInputStream())));
			} else {
				in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			}
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			throw new WashException("发送 POST 请求出现异常！", e);
		}
		// 使用finally块来关闭输出流、输入流
		finally {
			try {
				if (out != null) {
					out.close();
				}
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				throw new WashException("发送 POST 请求出现异常！", e);
			}
		}
		return result;
	}

	public static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return value;
        }
    }
    
	public static String md5(String md5) {
		try {
			java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
			byte[] array = md.digest(md5.getBytes());
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < array.length; ++i) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
			}
			return sb.toString();
		} catch (java.security.NoSuchAlgorithmException e) {
		}
		return null;
	}

	public static long parseIp(String address) {
		long result = 0;

		// iterate over each octet
		for (String part : address.split(Pattern.quote("."))) {
			// shift the previously parsed bits over by 1 byte
			result = result << 8;
			// set the low order bits to the current octet
			result |= Integer.parseInt(part);
		}
		return result;
	}
	

	public static boolean isNumeric(String str) {
	  return str.matches("-?\\d+(\\.\\d+)?");
	}
	
    public static String queryString(Map<String, Object> values) {
        StringBuilder sbuf = new StringBuilder();
        String separator = "";

        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Object entryValue = entry.getValue();
            if (entryValue instanceof Object[]) {
                for (Object value : (Object[]) entryValue) {
                    appendParam(sbuf, separator, entry.getKey(), value);
                    separator = "&";
                }
            } else if (entryValue instanceof Iterable) {
                for (Object multiValue : (Iterable) entryValue) {
                    appendParam(sbuf, separator, entry.getKey(), multiValue);
                    separator = "&";
                }
            } else {
                appendParam(sbuf, separator, entry.getKey(), entryValue);
                separator = "&";
            }
        }

        return sbuf.toString();
    }
    
    private static void appendParam(StringBuilder sbuf, String separator, String entryKey, Object value) {
        String sValue = value == null ? "" : String.valueOf(value);
        sbuf.append(separator);
        sbuf.append(urlEncode(entryKey));
        sbuf.append('=');
        sbuf.append(urlEncode(sValue));
    }
    
	public static HashMap<String, String> splitQuery(URL url) throws UnsupportedEncodingException {
		HashMap<String, String> query_pairs = new LinkedHashMap<String, String>();
		String query = url.getQuery();
		if (query != null) {
			String[] pairs = query.split("&");
			if (pairs.length > 1) {
				for (String pair : pairs) {
					String[] attr = pair.split("=");
					String val = "";
					if (attr.length > 1) {
						if (!attr[1].equals("")) {
							val = URLDecoder.decode(attr[1].replaceAll("%", "%25"), "UTF-8");
						}
						query_pairs.put(attr[0], val);
					}
				}
			}
		}
		return query_pairs;
	}
	

	public static String getJsonIP(String ipstr) {
		String ipjson = sendPost("http://cdh180:8080/", "ip=" + ipstr);
//		try {
//			URL url = new URL("http://cdh180:8080/?ip=" + ipstr);
//			InputStream is = url.openConnection().getInputStream();
//			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//			ipjson = reader.readLine();
//			reader.close();			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		return ipjson;
	}

	public static String getJsonUA(String ua) {
		String uajson = "";
//			URL url = new URL("http://cdh180:8080/?ua=" + URLEncoder.encode(ua, "utf-8"));
//			InputStream is = url.openConnection().getInputStream();
//			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//			uajson = reader.readLine();
//			reader.close();
		uajson = sendPost("http://cdh180:8080/", "ua=" + urlEncode(ua));
		return uajson;
	}	

    /**
     * Convert a byte array to a JSONObject.
     * @param bytes a UTF-8 encoded string representing a JSON object.
     * @return the parsed object
     * @throws WashException in case of error (usually a parsing error due to invalid JSON)
     */
    public static JSONObject toJsonObject(byte[] bytes) {
        String json;
        try {
            json = new String(bytes, "utf-8");
            return new JSONObject(json);
        } catch (UnsupportedEncodingException e) {
            throw new WashException(e);
        } catch (JSONException e) {
            throw new WashException("payload is not a valid JSON object", e);
        }
    }

    /**
     * Convert a byte array to a JSONArray.
     * @param bytes a UTF-8 encoded string representing a JSON array.
     * @return the parsed JSON array
     * @throws WashException in case of error (usually a parsing error due to invalid JSON)
     */
    public static JSONArray toJsonArray(byte[] bytes) {
        String json;
        try {
            json = new String(bytes, "utf-8");
            return new JSONArray(json);
        } catch (UnsupportedEncodingException e) {
            throw new WashException(e);
        } catch (JSONException e) {
            throw new WashException("payload is not a valid JSON array", e);
        }
    }
}
