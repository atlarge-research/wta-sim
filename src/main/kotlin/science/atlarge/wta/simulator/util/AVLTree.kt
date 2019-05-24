package science.atlarge.wta.simulator.util

class AVLTree<T>(
        private val comparator: Comparator<T>
) : Iterable<T> {

    private class Node<T>(val value: T) {
        var left: Node<T>? = null
        var right: Node<T>? = null
        var parent: Node<T>? = null
        var height: Int = 1
            private set
        var balance: Int = 0
            private set

        fun updateHeightAndBalance() {
            val leftHeight = left?.height ?: 0
            val rightHeight = right?.height ?: 0
            height = maxOf(leftHeight, rightHeight) + 1
            balance = rightHeight - leftHeight
        }

        fun replaceChild(child: Node<T>, newChild: Node<T>?) {
            if (left === child) left = newChild
            else right = newChild
        }
    }

    private var root: Node<T>? = null
    var size = 0
        private set

    fun insert(value: T): Boolean {
        val newNode = Node(value)
        if (root == null) {
            root = newNode
            size++
            return true
        } else {
            var node = root!!
            while (true) {
                val c = comparator.compare(value, node.value)
                when {
                    c < 0 -> {
                        val leftChild = node.left
                        if (leftChild == null) {
                            newNode.parent = node
                            node.left = newNode
                            rebalance(node)
                            size++
                            return true
                        } else {
                            node = leftChild
                        }
                    }
                    c > 0 -> {
                        val rightChild = node.right
                        if (rightChild == null) {
                            newNode.parent = node
                            node.right = newNode
                            rebalance(node)
                            size++
                            return true
                        } else {
                            node = rightChild
                        }
                    }
                    else -> return false
                }
            }
        }
    }

    fun remove(value: T): Boolean {
        // Find node for the given value
        var node = root
        while (node != null && node.value != value) {
            node = if (comparator.compare(value, node.value) < 0) {
                node.left
            } else {
                node.right
            }
        }
        if (node == null) return false

        // Remove the node
        if (node.height == 1) {
            // Special case 1: the node does not have children
            node.parent?.replaceChild(node, null)
            if (node.parent == null) {
                root = null
            } else {
                rebalance(node.parent!!)
            }
        } else if (node.right == null) {
            // Special case 2: the node does not have a right child
            val child = node.left!!
            child.parent = node.parent
            if (child.parent == null) {
                root = child
            } else {
                child.parent!!.replaceChild(node, child)
                rebalance(child.parent!!)
            }
        } else {
            // If a node has both a left and right subtree, find the next larger node to replace it
            var replacement = node.right!!
            while (replacement.left != null) {
                replacement = replacement.left!!
            }
            // Replace the next larger node with its right child (if any)
            val rightChild = replacement.right
            rightChild?.parent = replacement.parent
            replacement.parent!!.replaceChild(replacement, rightChild)
            // Rebalance the parent node after swapping one of its children
            rebalance(replacement.parent!!)
            // Swap the node to be removed with the next larger node that has been removed
            replacement.left = node.left
            replacement.left?.parent = replacement
            replacement.right = node.right
            replacement.right?.parent = replacement
            replacement.parent = node.parent
            if (replacement.parent == null) {
                root = replacement
            } else {
                replacement.parent!!.replaceChild(node, replacement)
            }
            replacement.updateHeightAndBalance()
        }
        node.parent = null
        node.left = null
        node.right = null
        size--
        return true
    }

    override fun iterator(): Iterator<T> {
        val firstNode = if (root == null) {
            null
        } else {
            var n = root!!
            while (n.left != null) {
                n = n.left!!
            }
            n
        }
        return ForwardIterator(firstNode)
    }

    fun iteratorFrom(value: T): Iterator<T> {
        val firstNode = findRight(value)
        return ForwardIterator(firstNode)
    }

    fun reverseIterator(): Iterator<T> {
        val firstNode = if (root == null) {
            null
        } else {
            var n = root!!
            while (n.right != null) {
                n = n.right!!
            }
            n
        }
        return BackwardIterator(firstNode)
    }

    fun reverseIteratorFrom(value: T): Iterator<T> {
        val firstNode = findLeft(value)
        return BackwardIterator(firstNode)
    }

    private fun findLeft(value: T): Node<T>? {
        var leftNode: Node<T>? = null
        var currentNode = root
        while (currentNode != null) {
            val c = comparator.compare(value, currentNode.value)
            when {
                c == 0 -> return currentNode
                c > 0 -> {
                    leftNode = currentNode
                    currentNode = currentNode.right
                }
                c < 0 -> {
                    currentNode = currentNode.left
                }
            }
        }
        return leftNode
    }

    private fun findRight(value: T): Node<T>? {
        var rightNode: Node<T>? = null
        var currentNode = root
        while (currentNode != null) {
            val c = comparator.compare(value, currentNode.value)
            when {
                c == 0 -> return currentNode
                c < 0 -> {
                    rightNode = currentNode
                    currentNode = currentNode.left
                }
                c > 0 -> {
                    currentNode = currentNode.right
                }
            }
        }
        return rightNode
    }

    private fun rebalance(node: Node<T>) {
        val oldHeight = node.height
        // Compute height difference between subtrees
        node.updateHeightAndBalance()
        // If the tree is unbalanced, rotate the current node
        val newNode = when {
            node.balance == -2 -> {
                if (node.left!!.balance == 1) {
                    rotateLeft(node.left!!)
                }
                rotateRight(node)
            }
            node.balance == 2 -> {
                if (node.right!!.balance == -1) {
                    rotateRight(node.right!!)
                }
                rotateLeft(node)
            }
            else -> node
        }
        // Propagate height and balance changes up the tree
        if (newNode.height != oldHeight && newNode.parent != null) {
            rebalance(newNode.parent!!)
        }
    }

    private fun rotateLeft(pivot: Node<T>): Node<T> {
        val newPivot = pivot.right!!
        // Update new pivot's parent
        newPivot.parent = pivot.parent
        if (newPivot.parent == null) {
            root = newPivot
        } else {
            newPivot.parent!!.replaceChild(pivot, newPivot)
        }
        // Attach new root's left child as old root's right child
        pivot.right = newPivot.left
        pivot.right?.let { it.parent = pivot }
        // Attach old root as new root's left child
        newPivot.left = pivot
        pivot.parent = newPivot
        // Recompute balance of both nodes
        pivot.updateHeightAndBalance()
        newPivot.updateHeightAndBalance()
        return newPivot
    }

    private fun rotateRight(pivot: Node<T>): Node<T> {
        val newPivot = pivot.left!!
        // Update new pivot's parent
        newPivot.parent = pivot.parent
        if (newPivot.parent == null) {
            root = newPivot
        } else {
            newPivot.parent!!.replaceChild(pivot, newPivot)
        }
        // Attach new root's right child as old root's left child
        pivot.left = newPivot.right
        pivot.left?.let { it.parent = pivot }
        // Attach old root as new root's right child
        newPivot.right = pivot
        pivot.parent = newPivot
        // Recompute balance of both nodes
        pivot.updateHeightAndBalance()
        newPivot.updateHeightAndBalance()
        return newPivot
    }

    private fun checkInvariant(node: Node<T>) {
        if (node.left != null) {
            checkInvariant(node.left!!)
            require(comparator.compare(node.left!!.value, node.value) < 0)
        }
        if (node.right != null) {
            checkInvariant(node.right!!)
            require(comparator.compare(node.value, node.right!!.value) < 0)
        }
        node.updateHeightAndBalance()
        require(node.balance in -1..1)
    }

    private class ForwardIterator<T>(initialNode: Node<T>?) : Iterator<T> {

        private var nextNode = initialNode

        override fun hasNext(): Boolean {
            return nextNode != null
        }

        override fun next(): T {
            val res = nextNode?.value ?: throw NoSuchElementException()
            findNext()
            return res
        }

        private fun findNext() {
            if (nextNode!!.right != null) {
                // Case 1: the current node has a right child
                // Get the leftmost node of the right subtree
                var newNode = nextNode!!.right!!
                while (newNode.left != null) {
                    newNode = newNode.left!!
                }
                nextNode = newNode
            } else {
                // Case 2: the current node does not have a larger child
                // Find the first larger ancestor, if any
                // (i.e., the first ancestor for which the current node is in its left subtree)
                var currentNode = nextNode!!
                var parentNode = currentNode.parent
                while (parentNode != null && currentNode === parentNode.right) {
                    currentNode = parentNode
                    parentNode = parentNode.parent
                }
                nextNode = parentNode
            }
        }

    }

    private class BackwardIterator<T>(initialNode: Node<T>?) : Iterator<T> {

        private var nextNode = initialNode

        override fun hasNext(): Boolean {
            return nextNode != null
        }

        override fun next(): T {
            val res = nextNode?.value ?: throw NoSuchElementException()
            findNext()
            return res
        }

        private fun findNext() {
            if (nextNode!!.left != null) {
                // Case 1: the current node has a left child
                // Get the rightmost node of the left subtree
                var newNode = nextNode!!.left!!
                while (newNode.right != null) {
                    newNode = newNode.right!!
                }
                nextNode = newNode
            } else {
                // Case 2: the current node does not have a smaller child
                // Find the first smaller ancestor, if any
                // (i.e., the first ancestor for which the current node is in its right subtree)
                var currentNode = nextNode!!
                var parentNode = currentNode.parent
                while (parentNode != null && currentNode === parentNode.left) {
                    currentNode = parentNode
                    parentNode = parentNode.parent
                }
                nextNode = parentNode
            }
        }

    }

}