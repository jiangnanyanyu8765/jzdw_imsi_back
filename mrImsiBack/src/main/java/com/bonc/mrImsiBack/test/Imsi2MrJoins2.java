package com.bonc.mrImsiBack.test;

import com.bonc.decodeMrXdr.entity.S1MMEXdr;
import mrLocateV2.bsparam.Checker;
import mrLocateV2.common.FileManager;
import mrLocateV2.mrdata.MrPoint;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/***
 * 处理IMSI向MR回填的代码，思路是先按照一次业务的MRS（两个MR时间间隔需要满足小于2*周期+1，即只允许丢掉一个MR），然后再对这个MRS进行回填。
 * 回填的搜索窗口默认为3分钟（测试结果为回填率52.77%,当设置为30秒时，回填率为52.04%，如果对MR数量要求不高的情况下可以设置小些）
 * @author Lideqiang
 *
 */
public class Imsi2MrJoins2 {
	private int segmentWindow;//mrs分割时间窗口（相邻两个mr时间大于该值，则分割为另外一个mrs）
	private int searchWindow;//参考MR搜索XDR的时间窗口
	Map<Long, List<S1MMEXdr>> xdrListMap = new HashMap<Long, List<S1MMEXdr>>();
	
	public void setSegmentWindow(int segmentWindow) {this.segmentWindow = segmentWindow;}
	public void setSearchWindow(int searchWindow) {this.searchWindow = searchWindow;}
	
	public Imsi2MrJoins2() {
		segmentWindow = 5012*2+1;//搜索2个mr周期内的XDR
		searchWindow = 3*60*1000;//默认为3分钟
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

						xdrList.add(xdr);
						xdrCount++;
					}

				}

			} finally {
				br.close();
			}
		}

		System.out.println("xdr count = " + xdrCount);
	}

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

	/**
	 * 将mrs（已经按照时间从小到大排序）分割生成包含多个业务lists的Map
	 * @param mrPointList 输入mrs
	 * @return 包含多个业务lists的Map(标识-list)
	 */
	@SuppressWarnings("unchecked")
	public Map<Integer, List<MrPoint>> segment2Lists(List<MrPoint> mrPointList){
		//保存生成结果
		Map<Integer, List<MrPoint>> mrPointListMap = new TreeMap<Integer, List<MrPoint>>();
		int listNo = 0;//list的序号
		ArrayList<MrPoint> bufferList = new ArrayList<MrPoint>();
		//执行分割
		int size = mrPointList.size();
		for(int i=0; i<size; i++) {
			if(i==0||mrPointList.get(i).getTimeStamp()
					-mrPointList.get(i-1).getTimeStamp()<segmentWindow) {
				bufferList.add(mrPointList.get(i));
			}
			else {
				mrPointListMap.put(listNo, (ArrayList<MrPoint>)bufferList.clone());
				//清空，并添加下一个list的mrPoint
				bufferList.clear();
				bufferList.add(mrPointList.get(i));
				listNo++;//递增
			}
		}
		//如果bufferList中有值，则填充到Map中
		if(!bufferList.isEmpty()) {
			mrPointListMap.put(listNo, (ArrayList<MrPoint>)bufferList.clone());
		}
		//返回分割结果
		return mrPointListMap;
	}
	
	/**
	 * 对于一次业务的mrs，将距离其任意一个mr最近的xdr imsi赋值给整个mrs集合
	 * @param mrPointList 一次业务的mrs
	 * @param xdrList xdr集合（对应s1apid+enodeBid）
	 * @return true:回填成功，false:没有得到IMSI
	 */
	public boolean imsi2SegmentedMrList(List<MrPoint> mrPointList, List<S1MMEXdr> xdrList) {
		//入口判断
		if(mrPointList==null) {
			throw new NullPointerException("mrPointList未初始化！");
		}		
		if(xdrList==null) {
			throw new NullPointerException("xdrList未初始化！");
		}		
		String imsi = null;//初始化为null
		int minDeltaTime = Integer.MAX_VALUE;
		for(MrPoint mrPoint:mrPointList) {
			for(S1MMEXdr xdr:xdrList) {
				long mrTime = mrPoint.getTimeStamp();
				long xdrTime = xdr.getStartTime();
				int deltaTime = (int)Math.abs(mrTime-xdrTime);
				//时间有值，时间差搜索窗内，imsi有值
				if(Checker.isAssigned(mrTime)&&Checker.isAssigned(xdrTime)
						&&deltaTime<searchWindow
						&&xdr.getImsi()!=null){
					if(minDeltaTime>deltaTime)	imsi = xdr.getImsi(); 
				}
			}
		}
		//将imsi赋值给整个list
		if(imsi!=null) {
			for(MrPoint mrPoint:mrPointList) mrPoint.setImsi(imsi);
			return true;
		}
		else return false;
	}

	public Map<Long,List<MrKey>> createImsiMrs(String filePath) throws IOException {
		//保存提取结果
		Map<Long,List<MrKey>> map = new HashMap<>();
		List<MrKey> mrKeyList = new ArrayList<>();
		List<String> pathNameList = new ArrayList<String>();
		FileManager.getAllFileName(filePath, "", pathNameList);
		for(String mrPathName : pathNameList){
			mrKeyList.addAll(this.importMrPointList(mrPathName));
		}
//		System.out.println("======");
//		for (int i=0; i<mrKeyList.size(); i++) {
//			System.out.println(mrKeyList.get(i).getTimestamp());
//		}
//		System.out.println("======");
		Collections.sort(mrKeyList);
//		for (int i=0; i<mrKeyList.size(); i++) {
//			System.out.println(mrKeyList.get(i).getTimestamp());
//		}
//		System.out.println("======");
		for (MrKey mrKey : mrKeyList) {
			List<MrKey> list = map.get(mrKey.getMr().getMmeUeS1Apid());
			if (list == null) {
				list = new ArrayList<>();
				map.put(mrKey.getMr().getMmeUeS1Apid(), list);
			}
			list.add(mrKey);
		}
		return map;
	}

	/**
	 * 根据文件中的mr生成mrPointList,主要该list可能包含多次业务的mrs
	 * @param mrFilePathName 文件名称
	 * @return mrPointList 生成的mrs
	 * @throws IOException
	 */
	public List<MrKey> importMrPointList(String mrFilePathName) throws IOException {
		//保存提取结果
		List<MrKey> mrPointList = new ArrayList<>();
		BufferedReader br = new BufferedReader(new FileReader(mrFilePathName));
		try {
			String strMrParam = null;
			while ((strMrParam = br.readLine()) != null) {
				//提取mrPoint内容
				MrPoint mrPoint = new MrPoint();
				mrPoint.stringTo(strMrParam);
				MrKey mrKey = new MrKey(mrPoint);
				//保存
				mrPointList.add(mrKey);
			}
		} finally {
			br.close();
		}
		return mrPointList;
	}

	/**
	 * 根据文件中的mr生成mrPointList,主要该list可能包含多次业务的mrs
	 * @param mrFilePathName 文件名称
	 * @return mrPointList 生成的mrs
	 * @throws IOException
	 */
	public List<MrPoint> import2SortedMrPointList(String mrFilePathName) throws IOException {
		//保存提取结果
		List<MrPoint> mrPointList = new ArrayList<MrPoint>();
		//缓存
		MrPoint mrPoint = new MrPoint();    	
		BufferedReader br = new BufferedReader(new FileReader(
				mrFilePathName));
        try {
     	   String strMrParam = null;
         while ((strMrParam = br.readLine()) != null) {
     		//提取mrPoint内容
     		mrPoint.stringTo(strMrParam);
     		//保存
			//mrPointList.add((MrPoint)mrPoint.clone());
			insertByTime(mrPointList, (MrPoint)mrPoint.clone());
         }
        } finally {
            br.close();
       }
		return mrPointList;
	}
	
	/**
	 * 将mrPoint按照时间从小到大顺序插入到列表中
	 * @param mrPointList mr列表
	 * @param mrPoint mr
	 */
	public void insertByTime(List<MrPoint> mrPointList, MrPoint mrPoint) {
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

	public static void main(String[] args) throws IOException {
		//路径设置
		String mrfilePath = "C:\\Users\\wtrover\\Desktop\\山西\\mr_xdr_test\\mr221112";
		String xdrfilePath = "C:\\Users\\wtrover\\Desktop\\山西\\mr_xdr_test\\xdr221112";
		//String xdrfilePathname = "E:\\项目管理\\山西移动\\IMSI回填验证\\山西全数据\\s1mme-20190404\\timeTest\\221112-151056479.txt";
		Imsi2MrJoins2 checker = new Imsi2MrJoins2();
		Map<Long,List<MrKey>> map = checker.createImsiMrs(mrfilePath);
		checker.createS1MMEXdrListMap(xdrfilePath);
//		System.out.println(mrs.size());
//		for (Map.)

	}
}
