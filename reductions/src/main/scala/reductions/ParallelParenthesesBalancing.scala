package reductions

import scala.annotation._
import scala.math.min
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def code(ch: Char): Int = ch match {
      case '(' => 1
      case ')' => -1
      case _ => 0
    }
    var (i, acc) = (0, 0)
    while (i < chars.length && acc >= 0) {
      acc = acc + code(chars(i))
      i = i + 1
    }
    acc == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(from: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      var (i, a1, a2) = (from, arg1, arg2)
      while(i < until) {
        chars(i) match {
          case '(' => a1 = a1 + 1
          case ')' => if (a1 > 0) a1 = a1 - 1
                      else a2 = a2 + 1
          case _ => ()
        }
        i = i + 1
      }
      (a1, a2)
    }

    // @tailrec
    // def traverse(from: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
    //   if (from >= until) (arg1, arg2)
    //   else chars(from) match {
    //       case '(' => traverse(from +1, until, arg1 + 1, arg2)
    //       case ')' => if (arg1 > 0) traverse(from +1, until, arg1 - 1, arg2)
    //                   else traverse(from +1, until, arg1, arg2 + 1)
    //       case _ => traverse(from +1, until, arg1, arg2)
    //   }
    // }

    def reduce(from: Int, until: Int): (Int, Int) =
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = (from + until) / 2
        val ((a1, a2), (b1, b2)) = parallel(reduce(from, mid), reduce(mid, until))
        val matched = min(a1, b2)
        (a1 + b1 - matched, a2 + b2 - matched)
      }

    reduce(0, chars.length) == Tuple2(0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
