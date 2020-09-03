package stack;

/**
 * 运算式，从右侧遍历，获取计算结果
 */

public class StackCaclulater {
    public static void main(String[] args) {
        //define a string
        String caculater = "100+5*10+33+34*10";
        LinkedStack numStack = new LinkedStack(100);
        LinkedStack symStack = new LinkedStack(100);

       /* char c1 = '+';
        char c2 = '*';
        symStack.push(c1);
        int i = symStack.comparePreority(c2);
        System.out.println(i);*/

        int length = caculater.length();
        String num = "";
        for (int i = length-1; i >=0 ; i--) {
            char c = caculater.charAt(i);
            if (c >=48 && c <= 57){
                int num0 = c-48;
                num = num0+num;
                if (i>0){
                    if (caculater.charAt(i-1)>57 ||caculater.charAt(i-1)<48){
                        numStack.push(Integer.parseInt(num));
                        num = "";
                    }
                }else {
                    numStack.push(Integer.parseInt(num));
                }
            }else{
                if(symStack.isEmpty()){
                        symStack.push(c);
                    }else {
                        if (symStack.priority(c) < symStack.priority(symStack.peek())) {
                        int num1 = numStack.pop();
                        int num2 = numStack.pop();
                        char sym = (char) symStack.pop();
                        int out = numStack.caculater(num1, num2, sym);
                        numStack.push(out);
                        symStack.push(c);
                    }else{
                        symStack.push(c);
                    }
                }
            }
        }
        while (numStack.size()>1){
            int resut = numStack.caculater(numStack.pop(), numStack.pop(), (char) symStack.pop());
            numStack.push(resut);
        }

        int result = numStack.pop();
        System.out.println(result);

    }
}
