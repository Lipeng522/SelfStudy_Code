package node;

import sun.awt.windows.ThemeReader;

import java.util.Stack;

public class HeroNodeDemo {
    public static void main(String[] args) {
        SingleNodeLinked singleNode = new SingleNodeLinked();
        HeroNode heroNode1 = new HeroNode(1, "宋江");
        HeroNode heroNode2 = new HeroNode(4, "吴用");
        HeroNode heroNode3 = new HeroNode(7, "林冲");
        HeroNode heroNode4 = new HeroNode(2, "武松");

        singleNode.addWithOrder(heroNode1);
        singleNode.addWithOrder(heroNode2);
        singleNode.addWithOrder(heroNode3);
        singleNode.addWithOrder(heroNode4);

        SingleNodeLinked singleNode2= new SingleNodeLinked();
        HeroNode heroNode5 = new HeroNode(3, "宋江2");
        HeroNode heroNode6 = new HeroNode(6, "吴用2");
        HeroNode heroNode7 = new HeroNode(5, "林冲2");
        HeroNode heroNode8 = new HeroNode(8, "武松2");

        singleNode2.addWithOrder(heroNode5);
        singleNode2.addWithOrder(heroNode6);
        singleNode2.addWithOrder(heroNode7);
        singleNode2.addWithOrder(heroNode8);

        singleNode.linkedList();
        singleNode.reviseNode();
        System.out.println("           --    ");
        singleNode.linkedList();
        System.out.println(singleNode.size());
        singleNode.printRightIndex(3);
        System.out.println("+++++++++++++++++++++++++++++");

        SingleNodeLinked newSingleLink = singleNode.uionLinkWith(singleNode2);
        newSingleLink.linkedList();
        System.out.println(" ----------------  ");

        singleNode.linkedList();
        singleNode2.linkedList();

    }
}

class SingleNodeLinked{
    private HeroNode headNode = new HeroNode(0,"");

    public HeroNode getHeadNode() {
        return headNode;
    }


    /**
     * 从尾到头打印链表
     */
    public void reversePrint(){
        if (headNode.nextNode == null){
            return;
        }
        Stack<HeroNode> stack = new Stack<>();
        //定义移动变量
        HeroNode tmp = headNode.nextNode;
        while(tmp != null){
            stack.push(tmp);
            tmp = tmp.nextNode;
        }
        while(!stack.empty())
            System.out.println(stack.pop());
    }

    /**
     * 取倒数第n个数据
     */
    public void printRightIndex(int index){
        if (headNode.nextNode == null){
            return;
        }
        //获取链表的长度
        int size = size();
        if (index <=0 || index >size){
            System.out.println("NO index");
        }
        int location = size - index;
        //定义辅助变量
        HeroNode tmp = headNode.nextNode;
        //找位置
        for (int i = 0; i < location; i++) {
            tmp = tmp.nextNode;
        }
        System.out.println(tmp);
    }
    /**
     * 获取链表的长度
     */

    public int size(){
        if (headNode.nextNode == null){
            return 0;
        }
        //定义移动变量
        HeroNode tmp = headNode.nextNode;
        int size = 0;
        while(tmp !=null){
            size++;
            tmp=tmp.nextNode;
        }
        return size;
    }
    /**
     * 反转链表
     */
    public void reviseNode(){
        if (headNode.nextNode==null || headNode.nextNode.nextNode == null){
            return;
        }
        //创建辅助变量
        HeroNode reveseNode = new HeroNode(0,"");
        HeroNode tmp = headNode.nextNode;
        HeroNode next = null;
        while(tmp!= null){
            next = tmp.nextNode;
            tmp.nextNode = reveseNode.nextNode;
            reveseNode.nextNode =tmp;
            tmp =next;
        }
        headNode.nextNode = reveseNode.nextNode;
    }

    /**
     * 合并两个有序的单链表，合并之后依然有序
     */
    public SingleNodeLinked uionLinkWith(SingleNodeLinked linked){
        SingleNodeLinked newSingleLink = null;
        SingleNodeLinked beJoinLink = null;
        if (size() >= linked.size()){
            newSingleLink = this;
            beJoinLink = linked;
        }else{
            newSingleLink = linked;
            beJoinLink = this;
        }
        newSingleLink.linkedList();
        System.out.println("*********************");
        beJoinLink.linkedList();
        System.out.println("-------------------------------------------------");
        //边遍历边向另一个内部添加
        HeroNode tmp = beJoinLink.getHeadNode().nextNode;
        HeroNode next = null;
        while (tmp !=null){
            next = tmp.nextNode;
            newSingleLink.addWithOrder(tmp);
            tmp = next;
        }
        return newSingleLink;
    }

    /**
     * 修改数据的方法
     * @param heroNode
     */
    public void update(HeroNode heroNode){
        if (headNode.nextNode == null){
            return;
        }
        //创建辅助变量
        HeroNode tmp = headNode.nextNode;
        boolean flag = false;
        while (true){
            if (tmp.nextNode == null){
                break;
            }
            if (tmp.id == headNode.id){
                flag = true;
                break;
            }
            tmp = tmp.nextNode;
        }
        tmp.heroName = heroNode.heroName;
    }
    /**
     * 删除指定节点的数据
     * @param id
     */
    public void delete(int id){
        if (headNode.nextNode == null){
            return;
        }
        //创建辅助变量
        boolean flag = false;
        HeroNode tmp = headNode;
        while (true){
            if (tmp.nextNode == null){
                break;
            }
            if (tmp.nextNode.id == id){
                flag = true;
                break;
            }
            tmp = tmp.nextNode;
        }
        if (flag) {
            tmp.nextNode = tmp.nextNode.nextNode;
        }else {
            System.out.println("NOT FIND :"+id);
        }
    }

    /**
     * 遍历链表的方法
     */
    public void linkedList(){
        //创建一个变量
        HeroNode tmp = headNode.nextNode;
        while (tmp != null){
            System.out.println(tmp);
            tmp = tmp.nextNode;
        }
    }
    public void addWithOrder(HeroNode heroNode){
        if (headNode.nextNode == null){
            headNode.nextNode = heroNode;
            return;
        }
        //创建一个辅助变量
        HeroNode tmp = headNode.nextNode;
        //找位置
        while (true){
            if (tmp.nextNode == null){
                break;
            }
            if (tmp.nextNode.id > heroNode.id){
                break;
            }
            tmp=tmp.nextNode;
        }
        heroNode.nextNode = tmp.nextNode;
        tmp.nextNode = heroNode;
    }
    /**
     * 无序添加数据方法
     * @param heroNode
     */
    //增加节点数据方法
    public void add(HeroNode heroNode){
        //遍历找尾,定义一个变量
        HeroNode tmp = headNode;
        while(true){
            if (tmp.nextNode == null){
                break;
            }
            tmp = tmp.nextNode;
        }
        tmp.nextNode = heroNode;
    }
}

class HeroNode{
    public int id;
    public String heroName;
    public HeroNode nextNode;

    public HeroNode(int id, String heroName) {
        this.id = id;
        this.heroName = heroName;
    }

    @Override
    public String toString() {
        return "HeroNode{" +
                "id=" + id +
                ", heroName='" + heroName +"}";
    }
}
