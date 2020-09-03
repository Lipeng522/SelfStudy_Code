package insert_sorter;

/**
 * User:LP
 * Date:2020-08-13 11:01
 * Description:堆排序：时间复杂度O(NlogN),空间复杂度O(NlogN),不稳定
 * 原理：堆，基于完全二叉树，完全二叉树的特点：1、父节点值大于子节点 2、节点从左往右依次添加
 */
public class HeapSorter {
	public static void main(String[] args) {

		//1、创建数据
		int[] tree = new int[]{6,7,4,10,3,5,1,2,9};


		//5、调用堆排序
		HeapSort(tree,tree.length);

		//6、遍历
		for (int i : tree) {
			System.out.println(i);
		}
	}

	//4、堆排序
	private static void HeapSort(int[] tree, int n) {
		buildHeap(tree,n);
		for (int i = n-1; i >=0 ; i--) {
			swap(tree,i,0);
			heapify(tree,i,0);
		}
	}

	//2、建堆
	private static void buildHeap(int[] tree, int n) {
		int last = n -1;
		int lastTreeNode = (last -1 )/2;
		for (int i = lastTreeNode; i >=0 ; i--) {
			heapify(tree,n,i);
		}
	}

	//3、堆化
	private static void heapify(int[] tree, int n, int i) {
		if (i>=n){
			return;
		}

		int lf = 2*i + 1;
		int rt = 2*i + 2;
		int max = i;

		if (lf<n && tree[lf] >tree[max]){
			max = lf;
		}

		if (rt<n && tree[rt] > tree[max] ){
			max = rt;
		}

		if (max != i){
			swap(tree,i,max);
			heapify(tree,n,max);
		}
	}

	//3、交换
	public static void swap(int[] arr , int first , int second){
		int tmp = arr[first];
		arr[first] = arr[second];
		arr[second] = tmp;
	}
}
