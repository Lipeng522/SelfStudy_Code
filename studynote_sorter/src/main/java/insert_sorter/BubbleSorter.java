package insert_sorter;

/**
 * User:LP
 * Date:2020-08-12 20:54
 * Description:冒泡排序法 时间复杂度：时间复杂度 O(N) -O（N²）;空间复杂度 O(1);稳定
 * 原理：将数据与其后的依次比较，若比后者大，交换位置
 */
public class BubbleSorter {
	public static void main(String[] args) {

		//1、定义数组
		int[] arr = new int[]{3,4,2,1,5,9,8,7};

		//2、循环进行比较,若前大于后，则交换位置
		for (int i = 0; i < arr.length-1; i++) {
			for (int j = 0; j <arr.length-1-i ; j++) {
				if (arr[j]>arr[j+1]){
					int tmp = arr[j];
					arr[j] = arr[j+1];
					arr[j+1] = tmp;
				}
			}
		}

		//3、遍历数组校验
		for (int i : arr) {
			System.out.print(i + "  ");
		}
	}
}
