package com.mytt

import java.nio.charset.Charset

import io.netty.bootstrap._
import io.netty.buffer._
import io.netty.channel._
import io.netty.channel.nio._
import io.netty.channel.socket._
import io.netty.channel.socket.nio._
import io.netty.handler.codec._

import net.liftweb.json

object Main {
	final val host = "127.0.0.1"
	final val port = 2256

	def main(argv: Array[String]): Unit = {
		val clientCount = argv(0).toInt
		val testTime = argv(1).toLong
		val speed = argv(2).toInt

		val eventLoop = new NioEventLoopGroup()

		val state = new State(clientCount)

		var timeAcc = 0L
    val startTime = System.currentTimeMillis
    var clientsNeed = clientCount

    var lastStamp = startTime - speed
		while (!state.over && System.currentTimeMillis - startTime < testTime * 1000) {
			Thread.sleep(1)

      state.ping()

      if (clientsNeed > 0) {
        val elapsed = System.currentTimeMillis - lastStamp
        lastStamp = System.currentTimeMillis

        timeAcc += elapsed

        var cnt = timeAcc / speed
        timeAcc = timeAcc % speed

        while (cnt > 0 && clientsNeed > 0) {
          connect(state, eventLoop)
          cnt -= 1
          clientsNeed -= 1
        }

      }
		}

    println(s"===> connected: ${state.connected}, failed: ${state.failed} caught: ${state.caught}")

		eventLoop.shutdownGracefully()
	}

	class PingClient(val state: State, val ch: SocketChannel) extends ChannelInboundHandlerAdapter {
		var lastTime = System.currentTimeMillis
		var ctx: ChannelHandlerContext = _
    var seqId = 0L

		override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
			state.onException(this)
		}

		override def handlerAdded(ctx: ChannelHandlerContext) {
			this.ctx = ctx
		}

		def ping(): Unit = this.synchronized{
			if (ctx != null) {
				val now = System.currentTimeMillis
				if (now - lastTime > 2000) {
					val b = ctx.alloc.buffer()
					b.writeShort(254)
					b.writeBytes(s"[$seqId, 0]".getBytes)
					ctx.writeAndFlush(b)

          lastTime = now
          seqId += 1
				}
			}
		}

		override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
			val b = msg.asInstanceOf[ByteBuf]
      val catId = b.readShort()

      val category = catId >>> 12
      val msgId = catId & 0xFFF

      val body = b.toString(b.readerIndex(), b.readableBytes(), Charset.forName("UTF-8"))
      val jobj = json.parse(body)

      println(s"cat: $category, id: $msgId, body: $jobj")
		}
	}

	class State(val totalCount: Int) {
		var connected = 0
		var failed = 0

		var finished = 0

		var caught: Int = 0

		var connections = Set[PingClient]()

		def ping(): Unit = this.synchronized{
			connections.foreach{ _.ping()}
		}

		def onConnectedSuccess(client: PingClient): Unit = this.synchronized{
			connected += 1
			connections += client
		}

		def onException(client: PingClient): Unit = this.synchronized{
			finished += 1
			caught += 1
			connections -= client
		}

		def onConnectedFailed(): Unit = this.synchronized{
			failed += 1
			finished += 1
		}

		def onComplete(client: PingClient): Unit = this.synchronized{
			connections -= client
			finished += 1
		}

		def over = finished == totalCount
	}


	def connect(state: State, eventLoop: EventLoopGroup) {
		val b = new Bootstrap()
		b.group(eventLoop)
		 .channel(classOf[NioSocketChannel])
//		 .option(ChannelOption.SO_KEEPALIVE, true)
		 .handler(new ChannelInitializer[SocketChannel]() {
		 	override def initChannel(ch: SocketChannel): Unit = {

		 		val lenDecoder = new LengthFieldBasedFrameDecoder(65536, 0, 2, 0, 2)
        val lenEncoder = new MessageToByteEncoder[ByteBuf]() {
          override def encode(ctx: ChannelHandlerContext, bb: ByteBuf, out: ByteBuf): Unit = {
            val n = bb.readableBytes()
            out.writeShort(bb.readableBytes())
            out.writeBytes(bb)
          }
        }

		 		val client = new PingClient(state, ch)

		 		ch.pipeline.addLast(lenDecoder)
        ch.pipeline.addLast(lenEncoder)
		 		ch.pipeline.addLast(client)

        state.onConnectedSuccess(client)

		 	}
	 	 })

	 	b.connect(host, port).addListener(new ChannelFutureListener() {
	 		override def operationComplete(f: ChannelFuture): Unit = {
	 			if (f.cause() != null)
	 				state.onConnectedFailed()
	 		}

 		})
	}
}