package stack;


import java.util.ArrayList;


public class ArrayStack {
    public static void main(String[] args) {
        LinkedStack stack = new LinkedStack(4);
        stack.push(1);
        stack.push(2);
        stack.push(3);
        stack.push(4);
        stack.push(5);

/*        stack.list();
        System.out.println("---------------");
        //System.out.println(stack.peek());
        System.out.println(stack.pop());
        System.out.println(stack.pop());
        System.out.println(stack.pop());
        System.out.println(stack.pop());
        try {
            System.out.println(stack.peek());
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(stack.isEmpty());*/

        System.out.println(stack.size());
        stack.pop();
        System.out.println(stack.size());

    }
}

class Stack{
    int top = -1;
    ArrayList<Integer> stackArray ;
    int capacity;
    public Stack(int capacity){
        this.capacity = capacity ;
        stackArray = new ArrayList<>(capacity);
    }

    public void push(int value){
        if (top == capacity-1){
            System.out.println("栈满");
            return;
        }
        stackArray.add(value);
        top ++;
    }

    public Integer pop(){
        if (top == -1){
            throw new RuntimeException("栈空");
        }
        Integer value = stackArray.get(top);
        stackArray.remove(top);
        top--;
        return value;
    }
    public void list(){
        for (int i = top; i >-1 ; i--) {
            System.out.println(stackArray.get(i));
        }
    }
    public int peek(){
        if (top == -1){
            throw new RuntimeException("栈空");
        }
        return stackArray.get(top);
    }
}

class LinkedStack{
    Node header = new Node(0);
    int capacity;
    int size = 0;
    LinkedStack(int capacity){
        this.capacity = capacity;
    }

    void push(int value){
        if (size() == capacity){
            System.out.println("栈满");
            return;
        }

        Node newNode = new Node(value);
        newNode.next = header.next;
        header.next = newNode;
    }
    void list(){
        if (size() == 0){
            return;
        }
        Node mvNode = header.next;
        while (mvNode !=null){
            System.out.println(mvNode.value);
            mvNode = mvNode.next;
        }
    }
    int pop(){
        int value =0;
        if (size() == 0){
            throw new RuntimeException("空栈");
        }
        value = header.next.value;
        header.next = header.next.next;
        return value;
    }
    int peek(){
        if (size() == 0){
            throw new RuntimeException("空栈");
        }
        return header.next.value;
    }
    int size(){
        size =0;
        Node mvNode = header.next;
        while(mvNode != null){
            size ++;
            mvNode = mvNode.next;
        }
        return size;
    }

    boolean isEmpty(){
        return size() == 0;
    }

    int caculater(int num1,int num2,char sym){
        int value = 0;
        switch (sym){
            case '+':
                value =  num1+num2;
                break;
            case '-' :
                value =  num1 - num2;
                break;
            case '*':
                value = num1*num2;
                break;
            case '/':
                value = num1/num2;
        }
        return value;
    }

    public int priority(int c) {
        if (c == '+' || c == '-'){
            return 0;
        }else  {
            return 1;
        }
    }

    public int comparePreority(char c) {
        int i = priority(c) - priority(peek());
        return i;
    }
}

class Node {
    int value;
    Node next;
    Node(int value){
        this.value = value;
    }
}