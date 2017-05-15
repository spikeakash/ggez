package com.spnotes.kafka.simple;

/**
 * Created by akash.ka on 5/12/17.
 */
public class Final_Student extends Student implements Method_Interface {

    public void implementAllMethods(){
        this.category = "Student";
        findSum();
    }

    public void findSum() {
        for(int x : subjectsTaken)
            sumOfMarks += x;
    }

}
