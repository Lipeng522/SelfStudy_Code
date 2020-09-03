package insert_sorter;

/**
 * User:LP
 * Date:2020-08-12 21:29
 * Description:希尔排序：时间复杂度O(N)-O(N²),空间复杂度：O(1),不稳定
 * 原理：将数组分组，然后分组内使用插入排序，先大局移动，再细节调整
 */
public class ShellSorter {
	public static void main(String[] args) {

		//1、定义数组
		int[] arr = new int[]{3,4,2,1,5,9,8,7,6,0};


		//2、分组循环
		for (int gap = arr.length/2; gap > 0 ; gap /= 2) {

			//2.2 循环进行插入运算
			for (int i = 0; i <arr.length-gap; i++) {

				//2.1 分组内进行插入排序
				int mvIndex = gap+i;
				int curValue = arr[mvIndex];

				while ( mvIndex-gap >= 0 && arr[mvIndex-gap] >= curValue ){
					arr[mvIndex] = arr[mvIndex-gap];
					mvIndex-=gap;
				}
				arr[mvIndex] = curValue;
			}
		}

		//3、遍历打印
		for (int i : arr) {
			System.out.print(i + "  ");
		}
	}
}
