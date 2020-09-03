package insert_sorter;

/**
 * User:LP
 * Date:2020-08-12 21:28
 * Description:快速排序：时间复杂度 O(NlogN) -O（N²）;空间复杂度 O(logN);不稳定
 * 原理是：标记位和移动索引，将比标记位小的放前边，
 *        大的放后边，然后将标记位移动到分界点，并拆分成多个数组，递归调用
 */
public class QuickSorter {
	public static void main(String[] args) {

		//1、定义数组
		int[] arr = new int[]{3,4,2,1,5,9,8,7};

		//3、调用快速排序
		int head = 0;
		int end = arr.length;
		quickSort(arr,head,end);

		//4、遍历数组校验
		for (int i : arr) {
			System.out.print(i + "  ");
		}
	}

	//3、创建快速排序方法
	private static void quickSort(int[] arr, int head, int end) {
		if (head>=end){
			return;
		}
		int key = arr[head];
		int moveIndex = head;
		for (int i = head+1; i <end ; i++) {
			if (arr[i] < key){
				moveIndex ++;
				int tmp = arr[moveIndex];
				arr[moveIndex] = arr[i];
				arr[i] = tmp;
			}
		}
		arr[head] = arr[moveIndex];
		arr[moveIndex] = key;
		quickSort(arr,head,moveIndex);
		quickSort(arr,moveIndex+1,end);
	}
}
