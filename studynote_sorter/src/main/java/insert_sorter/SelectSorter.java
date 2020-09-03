package insert_sorter;

/**
 * User:LP
 * Date:2020-08-12 21:01
 * Description:选择排序：时间复杂度 O(N) -O（N²）;空间复杂度 O(1);不稳定
 */
public class SelectSorter {
	public static void main(String[] args) {

		//1、定义数组
		int[] arr = new int[]{3,4,2,1,5,9,8,7};

		//2、使用下标进行选择排序
		for (int i = 0; i < arr.length-1; i++) {

			//2.1、定义最小下标，然后将后边的元素依次和该下标的值比较，小于该值，则替换下标
			int minIndex =i;
			for (int j = i+1; j <arr.length ; j++) {
				if (arr[j] < arr[minIndex]){
					minIndex =j;
				}
			}

			//2.1、交换元素
			int tmp = arr[i];
			arr[i] = arr[minIndex];
			arr[minIndex] = tmp;
		}
		//3、遍历数组校验
		for (int i : arr) {
			System.out.print(i + "  ");
		}
	}
}
