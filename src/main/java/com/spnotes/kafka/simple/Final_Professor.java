package com.spnotes.kafka.simple;

/**
 * Created by akash.ka on 5/12/17.
 */
public class Final_Professor extends Professor implements Method_Interface {

    public void implementAllMethods(){
        this.category = "Professor";
        findSum();
    }

    public void findSum() {
        for(int x : subjectsTaught)
            sumOfHoursTaught += x;
    }
}
