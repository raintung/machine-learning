package DBN

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import breeze.linalg.{
  Matrix => BM,
  CSCMatrix => BSM,
  DenseMatrix => BDM,
  Vector => BV,
  DenseVector => BDV,
  SparseVector => BSV,
  axpy => brzAxpy,
  svd => brzSvd
}
import breeze.numerics.{
  exp => Bexp,
  tanh => Btanh
}

import scala.collection.mutable.ArrayBuffer
import java.util.Random
import scala.math._

/**
 * W��Ȩ��
 * b��ƫ��
 * c��ƫ��
 */
case class DBNweight(
  W: BDM[Double],
  vW: BDM[Double],
  b: BDM[Double],
  vb: BDM[Double],
  c: BDM[Double],
  vc: BDM[Double]) extends Serializable

/**
 * ���ò���
 */
case class DBNConfig(
  size: Array[Int],
  layer: Int,
  momentum: Double,
  alpha: Double) extends Serializable

/**
 * DBN(Deep Belief Network)
 */

class DBN(
  private var size: Array[Int],
  private var layer: Int,
  private var momentum: Double,
  private var alpha: Double) extends Serializable with Logging {
  //                  var size=Array(5, 10, 10)
  //                  var layer=3
  //                  var momentum=0.0
  //                  var alpha=1.0
  /**
   * size = architecture;         ����ṹ
   * layer = numel(nn.size);      �������
   * momentum = 0.0;              Momentum
   * alpha = 1.0;                 alpha
   */
  def this() = this(DBN.Architecture, 3, 0.0, 1.0)

  /** ����������ṹ. Default: [10, 5, 1]. */
  def setSize(size: Array[Int]): this.type = {
    this.size = size
    this
  }

  /** ���������������. Default: 3. */
  def setLayer(layer: Int): this.type = {
    this.layer = layer
    this
  }

  /** ����Momentum. Default: 0.0. */
  def setMomentum(momentum: Double): this.type = {
    this.momentum = momentum
    this
  }

  /** ����alpha. Default: 1. */
  def setAlpha(alpha: Double): this.type = {
    this.alpha = alpha
    this
  }

  /**
   * ����������磨Deep Belief Network��
   * ����ѵ��DBNtrain
   */
  def DBNtrain(train_d: RDD[(BDM[Double], BDM[Double])], opts: Array[Double]): DBNModel = {
    // �������� �㲥����
    val sc = train_d.sparkContext
    val dbnconfig = DBNConfig(size, layer, momentum, alpha)
    // ��ʼ��Ȩ��
    var dbn_W = DBN.InitialW(size)
    var dbn_vW = DBN.InitialvW(size)
    var dbn_b = DBN.Initialb(size)
    var dbn_vb = DBN.Initialvb(size)
    var dbn_c = DBN.Initialc(size)
    var dbn_vc = DBN.Initialvc(size)
    // ѵ����1��
    printf("Training Level: %d.\n", 1)
    val weight0 = new DBNweight(dbn_W(0), dbn_vW(0), dbn_b(0), dbn_vb(0), dbn_c(0), dbn_vc(0))
    val weight1 = RBMtrain(train_d, opts, dbnconfig, weight0)
    dbn_W(0) = weight1.W
    dbn_vW(0) = weight1.vW
    dbn_b(0) = weight1.b
    dbn_vb(0) = weight1.vb
    dbn_c(0) = weight1.c
    dbn_vc(0) = weight1.vc
    // ��ӡȨ��
    printf("dbn_W%d.\n", 1)
    val tmpw0 = dbn_W(0)
    for (i <- 0 to tmpw0.rows - 1) {
      for (j <- 0 to tmpw0.cols - 1) {
        print(tmpw0(i, j) + "\t")
      }
      println()
    }

    // ѵ����2�� �� n��
    for (i <- 2 to dbnconfig.layer - 1) {
      // ǰ�����x
      //  x = sigm(repmat(rbm.c', size(x, 1), 1) + x * rbm.W');
      printf("Training Level: %d.\n", i)
      val tmp_bc_w = sc.broadcast(dbn_W(i - 2))
      val tmp_bc_c = sc.broadcast(dbn_c(i - 2))
      val train_d2 = train_d.map { f =>
        val lable = f._1
        val x = f._2
        val x2 = DBN.sigm(x * tmp_bc_w.value.t + tmp_bc_c.value.t)
        (lable, x2)
      }
      // ѵ����i��
      val weighti = new DBNweight(dbn_W(i - 1), dbn_vW(i - 1), dbn_b(i - 1), dbn_vb(i - 1), dbn_c(i - 1), dbn_vc(i - 1))
      val weight2 = RBMtrain(train_d2, opts, dbnconfig, weighti)
      dbn_W(i - 1) = weight2.W
      dbn_vW(i - 1) = weight2.vW
      dbn_b(i - 1) = weight2.b
      dbn_vb(i - 1) = weight2.vb
      dbn_c(i - 1) = weight2.c
      dbn_vc(i - 1) = weight2.vc
      // ��ӡȨ��
      printf("dbn_W%d.\n", i)
      val tmpw1 = dbn_W(i - 1)
      for (i <- 0 to tmpw1.rows - 1) {
        for (j <- 0 to tmpw1.cols - 1) {
          print(tmpw1(i, j) + "\t")
        }
        println()
      }
    }
    new DBNModel(dbnconfig, dbn_W, dbn_b, dbn_c)
  }

  /**
   * ����������磨Deep Belief Network��
   * ÿһ�����������ѵ��rbmtrain
   */
  def RBMtrain(train_t: RDD[(BDM[Double], BDM[Double])],
    opts: Array[Double],
    dbnconfig: DBNConfig,
    weight: DBNweight): DBNweight = {
    val sc = train_t.sparkContext
    var StartTime = System.currentTimeMillis()
    var EndTime = System.currentTimeMillis()
    // Ȩ�ز�������
    var rbm_W = weight.W
    var rbm_vW = weight.vW
    var rbm_b = weight.b
    var rbm_vb = weight.vb
    var rbm_c = weight.c
    var rbm_vc = weight.vc
    // �㲥����
    val bc_config = sc.broadcast(dbnconfig)
    // ѵ����������
    val m = train_t.count
    // ����batch������
    val batchsize = opts(0).toInt
    val numepochs = opts(1).toInt
    val numbatches = (m / batchsize).toInt
    // numepochs��ѭ���Ĵ��� 
    for (i <- 1 to numepochs) {
      StartTime = System.currentTimeMillis()
      val splitW2 = Array.fill(numbatches)(1.0 / numbatches)
      var err = 0.0
      // ���ݷ���Ȩ�أ��������ÿ����������  
      for (l <- 1 to numbatches) {
        // 1 �㲥Ȩ�ز���
        val bc_rbm_W = sc.broadcast(rbm_W)
        val bc_rbm_vW = sc.broadcast(rbm_vW)
        val bc_rbm_b = sc.broadcast(rbm_b)
        val bc_rbm_vb = sc.broadcast(rbm_vb)
        val bc_rbm_c = sc.broadcast(rbm_c)
        val bc_rbm_vc = sc.broadcast(rbm_vc)

        //        // ��ӡȨ��
        //        println(i + "\t" + l)
        //        val tmpw0 = bc_rbm_W.value
        //        for (i <- 0 to tmpw0.rows - 1) {
        //          for (j <- 0 to tmpw0.cols - 1) {
        //            print(tmpw0(i, j) + "\t")
        //          }
        //          println()
        //        }

        // 2 ��������
        val train_split2 = train_t.randomSplit(splitW2, System.nanoTime())
        val batch_xy1 = train_split2(l - 1)
        //        val train_split3 = train_t.filter { f => (f._1 >= batchsize * (l - 1) + 1) && (f._1 <= batchsize * (l)) }
        //        val batch_xy1 = train_split3.map(f => (f._2, f._3))

        // 3 ǰ�����
        // v1 = batch;
        // h1 = sigmrnd(repmat(rbm.c', opts.batchsize, 1) + v1 * rbm.W');
        // v2 = sigmrnd(repmat(rbm.b', opts.batchsize, 1) + h1 * rbm.W);
        // h2 = sigm(repmat(rbm.c', opts.batchsize, 1) + v2 * rbm.W');
        // c1 = h1' * v1;
        // c2 = h2' * v2;
        val batch_vh1 = batch_xy1.map { f =>
          val lable = f._1
          val v1 = f._2
          val h1 = DBN.sigmrnd((v1 * bc_rbm_W.value.t + bc_rbm_c.value.t))
          val v2 = DBN.sigmrnd((h1 * bc_rbm_W.value + bc_rbm_b.value.t))
          val h2 = DBN.sigm(v2 * bc_rbm_W.value.t + bc_rbm_c.value.t)
          val c1 = h1.t * v1
          val c2 = h2.t * v2
          (lable, v1, h1, v2, h2, c1, c2)
        }

        // 4 ����ǰ�����        
        // rbm.vW = rbm.momentum * rbm.vW + rbm.alpha * (c1 - c2)     / opts.batchsize;
        // rbm.vb = rbm.momentum * rbm.vb + rbm.alpha * sum(v1 - v2)' / opts.batchsize;
        // rbm.vc = rbm.momentum * rbm.vc + rbm.alpha * sum(h1 - h2)' / opts.batchsize;
        // W ���·���
        val vw1 = batch_vh1.map {
          case (lable, v1, h1, v2, h2, c1, c2) =>
            c1 - c2
        }
        val initw = BDM.zeros[Double](bc_rbm_W.value.rows, bc_rbm_W.value.cols)
        val (vw2, countw2) = vw1.treeAggregate((initw, 0L))(
          seqOp = (c, v) => {
            // c: (m, count), v: (m)
            val m1 = c._1
            val m2 = m1 + v
            (m2, c._2 + 1)
          },
          combOp = (c1, c2) => {
            // c: (m, count)
            val m1 = c1._1
            val m2 = c2._1
            val m3 = m1 + m2
            (m3, c1._2 + c2._2)
          })
        val vw3 = vw2 / countw2.toDouble
        rbm_vW = bc_config.value.momentum * bc_rbm_vW.value + bc_config.value.alpha * vw3
        // b ���·���
        val vb1 = batch_vh1.map {
          case (lable, v1, h1, v2, h2, c1, c2) =>
            (v1 - v2)
        }
        val initb = BDM.zeros[Double](bc_rbm_vb.value.cols, bc_rbm_vb.value.rows)
        val (vb2, countb2) = vb1.treeAggregate((initb, 0L))(
          seqOp = (c, v) => {
            // c: (m, count), v: (m)
            val m1 = c._1
            val m2 = m1 + v
            (m2, c._2 + 1)
          },
          combOp = (c1, c2) => {
            // c: (m, count)
            val m1 = c1._1
            val m2 = c2._1
            val m3 = m1 + m2
            (m3, c1._2 + c2._2)
          })
        val vb3 = vb2 / countb2.toDouble
        rbm_vb = bc_config.value.momentum * bc_rbm_vb.value + bc_config.value.alpha * vb3.t
        // c ���·���
        val vc1 = batch_vh1.map {
          case (lable, v1, h1, v2, h2, c1, c2) =>
            (h1 - h2)
        }
        val initc = BDM.zeros[Double](bc_rbm_vc.value.cols, bc_rbm_vc.value.rows)
        val (vc2, countc2) = vc1.treeAggregate((initc, 0L))(
          seqOp = (c, v) => {
            // c: (m, count), v: (m)
            val m1 = c._1
            val m2 = m1 + v
            (m2, c._2 + 1)
          },
          combOp = (c1, c2) => {
            // c: (m, count)
            val m1 = c1._1
            val m2 = c2._1
            val m3 = m1 + m2
            (m3, c1._2 + c2._2)
          })
        val vc3 = vc2 / countc2.toDouble
        rbm_vc = bc_config.value.momentum * bc_rbm_vc.value + bc_config.value.alpha * vc3.t

        // 5 Ȩ�ظ���
        // rbm.W = rbm.W + rbm.vW;
        // rbm.b = rbm.b + rbm.vb;
        // rbm.c = rbm.c + rbm.vc;
        rbm_W = bc_rbm_W.value + rbm_vW
        rbm_b = bc_rbm_b.value + rbm_vb
        rbm_c = bc_rbm_c.value + rbm_vc

        // 6 �������
        val dbne1 = batch_vh1.map {
          case (lable, v1, h1, v2, h2, c1, c2) =>
            (v1 - v2)
        }
        val (dbne2, counte) = dbne1.treeAggregate((0.0, 0L))(
          seqOp = (c, v) => {
            // c: (e, count), v: (m)
            val e1 = c._1
            val e2 = (v :* v).sum
            val esum = e1 + e2
            (esum, c._2 + 1)
          },
          combOp = (c1, c2) => {
            // c: (e, count)
            val e1 = c1._1
            val e2 = c2._1
            val esum = e1 + e2
            (esum, c1._2 + c2._2)
          })
        val dbne = dbne2 / counte.toDouble
        err += dbne
      }
      EndTime = System.currentTimeMillis()
      // ��ӡ�����
      printf("epoch: numepochs = %d , Took = %d seconds; Average reconstruction error is: %f.\n", i, scala.math.ceil((EndTime - StartTime).toDouble / 1000).toLong, err / numbatches.toDouble)
    }
    new DBNweight(rbm_W, rbm_vW, rbm_b, rbm_vb, rbm_c, rbm_vc)
  }

}

/**
 * NN(neural network)
 */
object DBN extends Serializable {

  // Initialization mode names
  val Activation_Function = "sigm"
  val Output = "linear"
  val Architecture = Array(10, 5, 1)

  /**
   * ��ʼ��Ȩ��
   * ��ʼ��Ϊ0
   */
  def InitialW(size: Array[Int]): Array[BDM[Double]] = {
    // ��ʼ��Ȩ�ز���
    // weights and weight momentum
    // dbn.rbm{u}.W  = zeros(dbn.sizes(u + 1), dbn.sizes(u));
    val n = size.length
    val rbm_W = ArrayBuffer[BDM[Double]]()
    for (i <- 1 to n - 1) {
      val d1 = BDM.zeros[Double](size(i), size(i - 1))
      rbm_W += d1
    }
    rbm_W.toArray
  }

  /**
   * ��ʼ��Ȩ��vW
   * ��ʼ��Ϊ0
   */
  def InitialvW(size: Array[Int]): Array[BDM[Double]] = {
    // ��ʼ��Ȩ�ز���
    // weights and weight momentum
    // dbn.rbm{u}.vW = zeros(dbn.sizes(u + 1), dbn.sizes(u));
    val n = size.length
    val rbm_vW = ArrayBuffer[BDM[Double]]()
    for (i <- 1 to n - 1) {
      val d1 = BDM.zeros[Double](size(i), size(i - 1))
      rbm_vW += d1
    }
    rbm_vW.toArray
  }

  /**
   * ��ʼ��ƫ������b
   * ��ʼ��Ϊ0
   */
  def Initialb(size: Array[Int]): Array[BDM[Double]] = {
    // ��ʼ��ƫ������b
    // weights and weight momentum
    // dbn.rbm{u}.b  = zeros(dbn.sizes(u), 1);
    val n = size.length
    val rbm_b = ArrayBuffer[BDM[Double]]()
    for (i <- 1 to n - 1) {
      val d1 = BDM.zeros[Double](size(i - 1), 1)
      rbm_b += d1
    }
    rbm_b.toArray
  }

  /**
   * ��ʼ��ƫ������vb
   * ��ʼ��Ϊ0
   */
  def Initialvb(size: Array[Int]): Array[BDM[Double]] = {
    // ��ʼ��ƫ������b
    // weights and weight momentum
    // dbn.rbm{u}.vb = zeros(dbn.sizes(u), 1);
    val n = size.length
    val rbm_vb = ArrayBuffer[BDM[Double]]()
    for (i <- 1 to n - 1) {
      val d1 = BDM.zeros[Double](size(i - 1), 1)
      rbm_vb += d1
    }
    rbm_vb.toArray
  }

  /**
   * ��ʼ��ƫ������c
   * ��ʼ��Ϊ0
   */
  def Initialc(size: Array[Int]): Array[BDM[Double]] = {
    // ��ʼ��ƫ������c
    // weights and weight momentum
    // dbn.rbm{u}.c  = zeros(dbn.sizes(u + 1), 1);
    val n = size.length
    val rbm_c = ArrayBuffer[BDM[Double]]()
    for (i <- 1 to n - 1) {
      val d1 = BDM.zeros[Double](size(i), 1)
      rbm_c += d1
    }
    rbm_c.toArray
  }

  /**
   * ��ʼ��ƫ������vc
   * ��ʼ��Ϊ0
   */
  def Initialvc(size: Array[Int]): Array[BDM[Double]] = {
    // ��ʼ��ƫ������c
    // weights and weight momentum
    // dbn.rbm{u}.vc = zeros(dbn.sizes(u + 1), 1);
    val n = size.length
    val rbm_vc = ArrayBuffer[BDM[Double]]()
    for (i <- 1 to n - 1) {
      val d1 = BDM.zeros[Double](size(i), 1)
      rbm_vc += d1
    }
    rbm_vc.toArray
  }

  /**
   * Gibbs����
   * X = double(1./(1+exp(-P)) > rand(size(P)));
   */
  def sigmrnd(P: BDM[Double]): BDM[Double] = {
    val s1 = 1.0 / (Bexp(P * (-1.0)) + 1.0)
    val r1 = BDM.rand[Double](s1.rows, s1.cols)
    val a1 = s1 :> r1
    val a2 = a1.data.map { f => if (f == true) 1.0 else 0.0 }
    val a3 = new BDM(s1.rows, s1.cols, a2)
    a3
  }

  /**
   * Gibbs����
   * X = double(1./(1+exp(-P)))+1*randn(size(P));
   */
  def sigmrnd2(P: BDM[Double]): BDM[Double] = {
    val s1 = 1.0 / (Bexp(P * (-1.0)) + 1.0)
    val r1 = BDM.rand[Double](s1.rows, s1.cols)
    val a3 = s1 + (r1 * 1.0)
    a3
  }

  /**
   * sigm�����
   * X = 1./(1+exp(-P));
   */
  def sigm(matrix: BDM[Double]): BDM[Double] = {
    val s1 = 1.0 / (Bexp(matrix * (-1.0)) + 1.0)
    s1
  }

  /**
   * tanh�����
   * f=1.7159*tanh(2/3.*A);
   */
  def tanh_opt(matrix: BDM[Double]): BDM[Double] = {
    val s1 = Btanh(matrix * (2.0 / 3.0)) * 1.7159
    s1
  }

}
