package com.bonc.mrImsiBack.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bonc.decodeMrXdr.entity.S1MMEXdr;
import mrLocateV2.common.FileManager;
import mrLocateV2.mrdata.MrPoint;
import org.apache.commons.lang.StringUtils;

public class Imsi2MrChecker {
	//保存提取结果
	Map<Long, List<S1MMEXdr>> xdrListMap = new HashMap<Long, List<S1MMEXdr>>();
	Map<String, List<S1MMEXdr>> xdrListMap2 = new HashMap<String, List<S1MMEXdr>>();
	Map<String, List<MrPoint>> mrListMap = new HashMap<String, List<MrPoint>>();
			
	public void createImsiMrs(String filePath) throws IOException {	
		List<String> pathNameList = new ArrayList<String>();
		FileManager.getAllFileName(filePath, "", pathNameList);
		int successCnt=0;
		int failCnt = 0;
		for(String mrPathName : pathNameList){
			BufferedReader br = new BufferedReader(new FileReader(
					mrPathName));
	        try {
	     	   String strMrParam = null;
             while ((strMrParam = br.readLine()) != null) {
            	 MrPoint mrPoint = new MrPoint();
            	 mrPoint.stringTo(strMrParam);
         		//提取mrPoint内容
            	 boolean isSuccess = fillImsi2Mr(mrPoint);
//            	 boolean isSuccess = fillImsi2Mr2(mrPoint);
            	 //System.out.println("s1apid: "+mrPoint.getMmeUeS1Apid()+", time: "+(mrPoint.getTimeStamp()/1000)+",imsi: "+ mrPoint.getImsi());
            	 if(isSuccess) {
            		 successCnt++;
	            	 List<MrPoint> mrList = mrListMap.get(mrPoint.getImsi());
	           		//保存mrPoint
	          		if(mrList==null) {
	          			mrList = new ArrayList<MrPoint>();
	          			mrListMap.put(mrPoint.getImsi(), mrList);
	          		}
	          		mrList.add(mrPoint);
	          		//System.out.println(mrPoint);
            	 }
            	 else {
            		 failCnt++;
            	 }

             }          
             
	        } finally {
	            br.close();
	       }
		}
		System.out.println("success Count: "+ successCnt+", fail Count: "+ failCnt);
	}
	
	public void analyze() {
		//打印mrs
		//for(Map.Entry<String, List<MrPoint>> entry:mrListMap)
	}
	
	
	public static void insertByTime(List<MrPoint> mrPointList, MrPoint mrPoint) {
		int size = mrPointList.size();
		if(size==0) {//如果是空列表，直接插入
			mrPointList.add(mrPoint);
		}
		else {
			//插入位置初始化
			int loc = size-1; 
			for(;loc>=0&&mrPointList.get(loc).getTimeStamp()>mrPoint.getTimeStamp(); ) {
				if(loc==size-1) mrPointList.add(mrPointList.get(loc));//后移一位
				else			mrPointList.set(loc+1, mrPointList.get(loc));//插入之后
				loc--;
			} 
			if(loc==size-1) mrPointList.add(mrPoint);//直接追加到列表
			else			mrPointList.set(++loc, mrPoint);//插入之后
 		} 	
	}
	
	
	
	
	
	
	public boolean fillImsi2Mr(MrPoint mrPoint){
		boolean isSuccess = false;
		//mrPoint按照IMSI分类
 		List<S1MMEXdr> xdrList = xdrListMap.get(mrPoint.getMmeUeS1Apid());
 		if(xdrList!=null) {
 			long minDeltaTime = Long.MAX_VALUE;
 			for(S1MMEXdr xdr:xdrList) {
 				long mrTime = mrPoint.getTimeStamp();
 				long xdrTime = xdr.getStartTime();
 				if(mrTime>0&&xdrTime>0
 						&&Math.abs(mrTime-xdrTime)<minDeltaTime
 						&&Math.abs(mrTime-xdrTime)<3*60*1000) {
 					minDeltaTime = Math.abs(mrTime-xdrTime);
 					mrPoint.setImsi(xdr.getImsi());
 					isSuccess = true;
 				}
 			}
 		}
 		
 		return isSuccess;
	}

	public boolean fillImsi2Mr2(MrPoint mrPoint){
		boolean isSuccess = false;
		int enbid = (int)(mrPoint.getScCell().getEci() / 256);
		List<S1MMEXdr> xdrList = xdrListMap2.get(enbid + "_" +mrPoint.getMmeUeS1Apid());
		if(xdrList!=null) {
			long minDeltaTime = Long.MAX_VALUE;
			for(S1MMEXdr xdr:xdrList) {
				long mrTime = mrPoint.getTimeStamp();
				long xdrTime = xdr.getStartTime();
				if(mrTime>0&&xdrTime>0
						&&Math.abs(mrTime-xdrTime)<minDeltaTime
						&&Math.abs(mrTime-xdrTime)<3*60*1000) {
					minDeltaTime = Math.abs(mrTime-xdrTime);
					mrPoint.setImsi(xdr.getImsi());
					isSuccess = true;
				}
			}
		}

		return isSuccess;
	}
	
	
	
	
	public void analyzeS1MMEXdrListMap() throws IOException {
	
        //分析
        List<S1MMEXdr> xdrList = xdrListMap.get(151056479l);  
        for(S1MMEXdr xdr2:xdrList) {
        	System.out.println((xdr2.getStartTime()/1000)+","+xdr2.getMmeUeS1Apid()+","+xdr2.getImsi());
        }
	}
	
	
	
    /**
     * 从所有字段中提取出需要的字段
     * @param strRawParam 包含所有字段的字符串
     * @param strParams 提取所需字段结果数组
     * @param vendor 厂家，对应不同数据格式
     * @return true:提取成功，false:提取失败
     */
    public S1MMEXdr createMmeXdr(String strParams){
    	//初始化为fasle
    	S1MMEXdr xdr = new S1MMEXdr();
    	//解析输入字符串
	   // String[] rawParams = strRawParam.split("\\|", -1);
	    String[] rawParams = StringUtils.splitByWholeSeparatorPreserveAllTokens(strParams, ",");//效率较高
	    //从XDR文件中读取各参数字段
	    if(rawParams.length>=1) {
	    	xdr.setImsi(rawParams[0]);
	    }
	    
	    if(rawParams.length>=2) {
	    	xdr.setScCellEci(Long.parseLong(rawParams[1]));
	    }
	    
	    if(rawParams.length>=3){
	    	xdr.setMmeUeS1Apid(Long.parseLong(rawParams[2]));    
	    }
	    
	    if(rawParams.length>=4){  
	    	xdr.setStartTime(Long.parseLong(rawParams[3]));
	    }

    	return xdr;
    }
	
	public void createS1MMEXdrListMap(String filePath) throws IOException {
		//缓存
		S1MMEXdr xdr = new S1MMEXdr();   	
		List<String> pathNameList = new ArrayList<String>();
		FileManager.getAllFileName(filePath, "", pathNameList);
		int xdrCount = 0;
		
		for(String mrPathName : pathNameList){
			BufferedReader br = new BufferedReader(new FileReader(
					mrPathName));
	        try {
	     	   String strMrParam = null;
             while ((strMrParam = br.readLine()) != null) {
         		//提取mrPoint内容
            	 xdr = new S1MMEXdr(); 
            	 xdr = createMmeXdr(strMrParam);

				 if (xdr.getImsi() == null || "\\N".equals(xdr.getImsi()) ||
						 xdr.getMmeUeS1Apid() == 4294967295L ||
						 xdr.getScCellEci() == 4294967295L) {

				 } else {
					 List<S1MMEXdr> xdrList = xdrListMap.get(xdr.getMmeUeS1Apid());
					 if(xdrList==null) {
						 xdrList = new ArrayList<S1MMEXdr>();
						 xdrListMap.put(xdr.getMmeUeS1Apid(), xdrList);
					 }

					 int enbid = (int)xdr.getScCellEci() / 256;
					 List<S1MMEXdr> xdrList2 = xdrListMap2.get(enbid + "_" +xdr.getMmeUeS1Apid());
					 if(xdrList2==null) {
						 xdrList2 = new ArrayList<S1MMEXdr>();
						 xdrListMap2.put(enbid + "_" +xdr.getMmeUeS1Apid(), xdrList2);
					 }
					 xdrList.add(xdr);
					 xdrList2.add(xdr);
					 xdrCount++;
				 }

             }          
             
	        } finally {
	            br.close();
	       }
		}

		System.out.println("xdr count = " + xdrCount);
	}
	
	/**
	 * 将mrPoint按照时间从小到大顺序插入到列表中
	 * @param mrPointList mr列表
	 * @param mrPoint mr
	 */
	public static void insertByTime(List<S1MMEXdr> xdrList, S1MMEXdr xdr) {
		int size = xdrList.size();
		if(size==0) {//如果是空列表，直接插入
			xdrList.add(xdr);
		}
		else {
			//插入位置初始化
			int loc = size-1; 
			for(;loc>=0&&xdrList.get(loc).getStartTime()>xdr.getStartTime(); ) {
				if(loc==size-1) xdrList.add(xdrList.get(loc));//后移一位
				else			xdrList.set(loc+1, xdrList.get(loc));//插入之后
				loc--;
			} 
			if(loc==size-1) xdrList.add(xdr);//直接追加到列表
			else			xdrList.set(++loc, xdr);//插入之后
 		} 	
	}
	
	
	
	
	

	public static void main(String[] args) throws IOException {
		//路径设置
		String mrfilePath = "C:\\Users\\wtrover\\Desktop\\山西\\mr_xdr_test\\mr221112";
		String xdrfilePath = "C:\\Users\\wtrover\\Desktop\\山西\\mr_xdr_test\\xdr221112";
		//String xdrfilePathname = "E:\\项目管理\\山西移动\\IMSI回填验证\\山西全数据\\s1mme-20190404\\timeTest\\221112-151056479.txt";
		Imsi2MrChecker checker = new Imsi2MrChecker();
		checker.createS1MMEXdrListMap(xdrfilePath);
		checker.createImsiMrs(mrfilePath);
		//checker.analyzeS1MMEXdrListMap();
		//checker.analyzeS1MMEXdrListMap(filePath);
	
	}
			
}
