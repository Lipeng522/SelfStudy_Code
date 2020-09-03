package insert_sorter;

import javax.xml.stream.events.EndDocument;

/**
 * User:LP
 * Date:2020-08-12 21:29
 * Description:归并排序：时间复杂度O(NlogN),空间复杂度：O(N),稳定
 * 原理：先分后合，先分解成单个元素，然后先合并成有序的子数组，在将有序的子数组合并
 */
public class MergeSorter {
	public static void main(String[] args) {

		//1、定义数组
		int[] arr = new int[]{3,4,2,1,5,9,8,7};

		//4、调用mergesort,指定左右侧索引
		int left = 0;
		int right = arr.length-1;
		mergeSort(arr,left,right);

		//5、遍历
		for (int i : arr) {
			System.out.print(i + "  ");
		}
	}

	//2、创建mergeSort方法
	private static void mergeSort(int[] arr, int left, int right) {

		//2.1 递归出口，当拆成只有一个元素时，停止拆分，开始聚合
		if (left>=right){
			return;
		}

		//2.1 获取拆分数组的中间值(
		int mid = (right+left)>>>1;

		//2.2 拆分左侧子数组和右侧子数组继续拆分
		mergeSort(arr,left,mid);
		mergeSort(arr,mid+1, right);

		//2.3 拆分完成之后开始聚合
		merge(arr,left,mid,right);
	}

	//3、创建merge方法
	private static void merge(int[] arr, int left, int mid, int right) {

		//3.1 创建一个储存数据的临时数组,指定与传入数组相同的长度
		int[] tmp = new int[right-left+1];

		//3.2 定义左侧移动索引和右侧移动索引,临时数组移动下标
		int lf = left;
		int rt = mid+1;
		int mvIndex = 0;

		//3.3 判断并进行合并
		while (lf <= mid && rt <= right){

			//3.3.1 先将两者小的去除放进临时数组
			if (arr[lf] <=arr[rt]){
				tmp[mvIndex++] = arr[lf++];
			}else {
				tmp[mvIndex++] = arr[rt++];
			}
		}

		//3.4 当一侧的数组为空时，直接将另一侧的数组放入临时数组
		while (lf <= mid){
			tmp[mvIndex++] = arr[lf++];
		}

		while (rt <= right){
			tmp[mvIndex++] = arr[rt++];
		}

		//3.5 将合并完的数组copy到原arr对应的位置
		for (int i = 0; i < tmp.length; i++) {
			arr[left+i] = tmp[i];
		}
	}
}
