/*
 * Copyright 2013-2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mitll.xdata;

import java.util.Arrays;

/**
 * Utility for generating the Cartesian product over several (ordered) sets.
 * 
 * It attempts to generate tuples with low indexes first.
 */
public class PrioritizedCartesianProduct {
    /** size of each ordered list */
    private int[] listSizes;

    /** number of ordered lists */
    private int numLists;

    /** index that must be included at least once in next set */
    private int highestIndex;

    /** which lists are currently fixed at highestIndex */
    private boolean[] fixedAtHighest;

    /** which lists can be fixed at highestIndex (i.e., have enough elements) */
    private boolean[] canBeFixedAtHighest;

    /** current index for each list */
    private int[] indices;

    public PrioritizedCartesianProduct(int[] listSizes) {
        this.listSizes = listSizes;

        numLists = listSizes.length;
        indices = new int[numLists];
        fixedAtHighest = new boolean[numLists];
        canBeFixedAtHighest = new boolean[numLists];
        
        reset();
    }
    
    public void reset() {
        // initialize state so that next() call will advance to 0,...,0
        highestIndex = 0;

        for (int i = 0; i < numLists; i++) {
            fixedAtHighest[i] = true;
            canBeFixedAtHighest[i] = true;
        }

        // make mask  T,...,T,F so that next() can increment to T,...,T to set all 0s in first group of indices
        fixedAtHighest[numLists - 1] = false;

        // initialize indices to 0,...,0,-1 so that first increment successfully advance group to 0,...,0
        for (int i = 0; i < numLists; i++) {
            indices[0] = 0;
        }
        indices[numLists - 1] = -1;
    }

    /**
     * Advances generator to next tuple.
     * 
     * @return true if next set if valid; false if there are no more sets
     */
    public boolean next() {
        // increment lists whose indices that aren't fixed at current highest index
        if (incrementNonFixed()) {
            return true;
        }

        // if can't, increment highest mask (change which indices are fixed at current highest index)
        if (incrementFixedMask()) {
            return true;
        }

        // if can't, increment highest index
        if (incrementHighestIndex()) {
            return true;
        }

        return false;
    }

    private boolean incrementNonFixed() {
        // start from left-most list (column)
        return incrementNonFixed(numLists - 1);
    }

    private boolean incrementNonFixed(int startIndex) {
        // Note: this could probably be modified so that mask 0,1,0,0,[fixed] would come before 0,0,1,1,[fixed] (to make smaller indices appear in earlier groups)

        // suppose:

        // listSizes = 6, 3, 7
        // highestIndex = 2
        // highestMask = 0, 1, 0
        // indices = 4, [2], 6 (where the 2 is fixed due to the highestMask)

        // then
        // indices = 5, [2], 0 since reached end of list 3 (which is size 7)
        // indices = 5, [2], 1
        // etc.

        if (startIndex < 0) {
            // attempt to carry failed (i.e., can't increment non-fixed columns anymore)
            return false;
        }
        
        for (int i = startIndex; i >= 0; i--) {
            if (!fixedAtHighest[i]) {
                // increment
                indices[i]++;
                
                // check for overflow in column (which can be caused by reaching highestIndex or list size)
                if (indices[i] == highestIndex || indices[i] == listSizes[i]) {
                    // attempt to carry (i.e., increment columns to the left)
                    indices[i] = 0;
                    return incrementNonFixed(i - 1);
                } else {
                    // simple increment was successful
                    return true;
                }
            }
        }

        // ran out of non-fixed columns to increment or carry to
        return false;
    }
    
    private boolean incrementFixedMask() {
        // start from left-most list (column)
        return incrementFixedMask(numLists - 1);
    }
    
    private boolean incrementFixedMask(int startIndex) {
        // Note: this could probably be modified so that mask 0,1,0,0 would come before 0,0,1,1 (to make smaller indices appear in earlier groups)
        
        // suppose:

        // listSizes = 6, 3, 7
        // highestIndex = 2
        // fixedAtHighest = 0, 1, 0
        // canBeFixedAtHighest = 1, 1, 1
        // indices = 5, [2], 6 (where the 2 is fixed due to the highestMask)

        // then incrementNonFixed() fails since can't carry anymore
        // 
        // fixedAtHighest = 0, 1, 1 (like adding 1 to a binary number)
        // indices = 0, [2], [2]
        
        if (startIndex < 0) {
            // attempt to carry failed (i.e., can't increment fixed mask anymore)
            return false;
        }
        
        for (int i = startIndex; i >= 0; i--) {
            if (!fixedAtHighest[i] && canBeFixedAtHighest[i]) {
                // simple increment
                fixedAtHighest[i] = true;
                initializeFixedMask();
                return true;
            } else if (canBeFixedAtHighest[i]) {
                // attempt to "carry"
                fixedAtHighest[i] = false;
                return incrementFixedMask(i - 1);
            }
        }
        
        // ran out of fixed columns to try to increment or carry to
        return false;
    }
    
    private void initializeFixedMask() {
        // System.out.print("setting fixedAtHighest = ");
        // for (int i = 0; i < numLists; i++) {
        // System.out.print(fixedAtHighest[i] ? "T," : "F,");
        // }
        // System.out.println();
        
        for (int i = 0; i < numLists; i++) {
            indices[i] = (fixedAtHighest[i] ? highestIndex : 0);
        }
    }
    
    private boolean incrementHighestIndex() {
        highestIndex++;
        
        // System.out.println("setting highestIndex = " + highestIndex);
        
        // initialized canBeFixedAtHighest
        for (int i = 0; i < numLists; i++) {
            canBeFixedAtHighest[i] = (highestIndex < listSizes[i]);
        }
        
        // initialize fixedAtHighest to false
        for (int i = numLists - 1; i >= 0; i--) {
            fixedAtHighest[i] = false;
        }
        
        // set leftmost fixedAtHighest that can be fixed to true
        for (int i = numLists - 1; i >= 0; i--) {
            if (canBeFixedAtHighest[i]) {
                fixedAtHighest[i] = true;
                initializeFixedMask();
                return true;
            }
        }
        
        // have reached end since no indices can be fixed to highest
        return false;
    }

    /**
     * @return current tuple of indexes into ordered lists
     */
    public int[] getIndices() {
        return Arrays.copyOf(indices, numLists);
    }
    
    public void displayIndices() {
        for (int i : indices) {
            System.out.print(i + ", ");
        }
        System.out.println();
    }

    public static void main(String[] args) {
        // PrioritizedCartesianProduct product = new PrioritizedCartesianProduct(new int[] { 3, 3, 3, 3 });
        // PrioritizedCartesianProduct product = new PrioritizedCartesianProduct(new int[] { 3, 2, 4 });
        PrioritizedCartesianProduct product = new PrioritizedCartesianProduct(new int[] { 6, 3, 7 });

        while (product.next()) {
            product.displayIndices();
        }

        System.out.println("done");
    }
}
