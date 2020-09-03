package insert_sorter;

/**
 * User:LP
 * Date:2020-08-12 20:42
 * Description:插入排序法：时间复杂度 O(N) -O（N²）;空间复杂度 O(1);稳定
 * 原理是：  那前边的值依次与标记的当前值比较，若比当前值大，
 *          就将大的值后移一位，然后继续与前边的比，
 *          直到前方无数据为止
 */
public class InsertSorter {
	public static void main(String[] args) {

		//1、定义数组
		int[] arr = new int[]{3,4,2,1,5,9,8,7};

		//2、寻找插入位置
		for (int i = 1; i < arr.length; i++) {

			//2.1、标记当前位置及当前值
			int index = i;
			int curValue = arr[i];

			//2.2、循环查找插入位置
			while (index-1 >= 0 && arr[index-1] >curValue){
				arr[index] = arr[index-1];
				index--;
			}
			//2.3、将对应值插入对应位置
			arr[index] = curValue;
		}

		//3、遍历数组校验
		for (int i : arr) {
			System.out.print(i + "  ");
		}
	}
}
