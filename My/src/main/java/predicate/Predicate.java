package predicate;

import lombok.Getter;
import lombok.ToString;
import plan.Plan;
import scan.Scan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ToString
public class Predicate {
    @Getter List<Term> terms;

    public static Predicate emptyPredicate=new Predicate(Arrays.asList(
            new Term(new Expression(new Constant<>(0)),new Expression(new Constant<>(1)))
    ));

    public Predicate(){
        this.terms=new ArrayList<>();
    }
    public Predicate(List<Term> terms){
        this.terms=terms;
    }

    public boolean isSatisfied(Scan scan){
        for(Term term:terms){
            if(!term.isSatisfied(scan)){
                return false;
            }
        }
        return true;
    }

    public int reductionFactor(Plan plan){
        int ret=1;
        for(Term term:terms){
            ret*=term.reductionFactor(plan);
        }
        return ret;
    }

    public void addTerm(Term term){
        terms.add(term);
    }

    public void removeTerm(Term term){
        terms.remove(term);
    }
}
