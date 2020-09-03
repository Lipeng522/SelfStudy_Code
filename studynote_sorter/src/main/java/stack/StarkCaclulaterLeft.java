package stack;

/**
 * 从左侧遍历实现公式计算
 */
public class StarkCaclulaterLeft {
    public static void main(String[] args) {
        String caculater = "100+5*10+33+34*10";
        LinkedStack numStack = new LinkedStack(100);
        LinkedStack symStack = new LinkedStack(100);

        String num = "";
        for (int i = 0; i < caculater.length(); i++) {
            if (caculater.charAt(i)>=48 && caculater.charAt(i) <= 57){
                int num0 = caculater.charAt(i)-48;
               num = num + num0;
               if (i != caculater.length()-1){
                   if (caculater.charAt(i+1)<48 || caculater.charAt(i+1) >57){
                       numStack.push(Integer.parseInt(num));
                       num ="";
                   }
                }else {
                   numStack.push(Integer.parseInt(num));
                   num = "";
               }
            }else{
                if (symStack.isEmpty()){
                    symStack.push(caculater.charAt(i));
                }else {
                    if (symStack.priority(caculater.charAt(i))<=symStack.priority(symStack.peek())){
                        int num2 = numStack.pop();
                        int num1 = numStack.pop();
                        char sym = (char)symStack.pop();
                        int result = numStack.caculater(num1, num2, sym);
                        numStack.push(result);
                        symStack.push(caculater.charAt(i));
                    }else {
                        symStack.push(caculater.charAt(i));
                    }
                }
            }
        }
        while (true){
            int num2 = numStack.pop();
            int num1 = numStack.pop();
            char sym = (char)symStack.pop();
            int result = numStack.caculater(num1, num2, sym);
            numStack.push(result);
            System.out.println(numStack.size());
            if (numStack.size() == 1){
                break;
            }
        }

        System.out.println(numStack.pop());
    }
}
