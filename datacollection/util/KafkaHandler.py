from pykafka import KafkaClient
import logging
import Queue
import socket


class KafkaHandler(logging.Handler):
	"""Kafka logger handler attempts to write python logs directly
	into specified kafka topic instead of writing them into file.
	
	Email on exceptions raised from delivery failure is supported with SMTTP
	"""

	def __init__(self, backup_file, hosts, zookeeper, topic, batch_size, mailhost, fromaddr, toaddrs, subject, 
		credentials=None, secure=None, timeout=5.0):
		
		logging.Handler.__init__(self)
		kafka_client = KafkaClient(hosts)
		# Kafka related instance variables
		self.backup_file = open(backup_file, 'w')
		self.topic = kafka_client.topics[topic]
		self.key = bytes(socket.gethostname())
		self.producer = topic.get_producer(
			delivery_reports=True,
			min_queued_messages=batch_size,
			max_queued_messages=batch_size * 1000,
			linger_ms=15000,
			block_on_queue_full=False)
		self.count = 0

		# SMTP related instance variables
		if isinstance(mailhost, (list, tuple)):
			self.mailhost, self.mailport = mailhost
		else:
			self.mailhost, self.mailport = mailhost, None
		if isinstance(credentials, (list, tuple)):
			self.username, self.password = credentials
		else:
			self.username = None
		self.fromaddr = fromaddr
		if isinstance(toaddrs, str):
			toaddrs = [toaddrs]
		self.toaddrs = toaddrs
		self.subject = subject
		self.secure = secure
		self.timeout = timeout

	def emit(self, record):
		""" This method receives logs as parameter record through 
		logging framework, send them to Kafka Cluster
		"""
		# drop kafka logging 
		if record.name == 'kafka':
			return
		try:
			msg = self.format(record)
			self.producer.produce(msg, partition_key=self.key)
			# check delivery of logs to kafka when we reach 2 * batch size records streamed 
			self.count += 1
			if self.count > (self.batch_size * 2):
				self.check_delivery()
				self.count = 0
		except AttributeError:
			self.write_backup("ERROR: Kafka Producer failed to back up to topic:", + str(self.topic))
			self.write_backup(msg)
			pass
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			# Log erros due queue full
			self.write_backup(msg)
			self.handleError(record)


	def getSubject(self, exception):
		"""
		sets the subject to set subject + exception + kafka topic
		"""
		# RFC 2822 Section 2.1.1: Email subject length is limited to the 78 character length standard
		return self.subject + " " self.topic + " " exception if len(self.subject) + len(exception) + len(self.topic) < 79 
			else self.subject + " " self.topic + " " + exception [:78 - len(self.subject) - len(self.topic)]


	def check_delivery(self):
		"""Checks the delivery reports from Kafka producer,
		failed reported will be written to backup file.
		"""
		while True:
			try:
				msg, exc = self.producer.get_delivery_report(block=False)
				if exc is not None:
					exception = repr(exc)
					self.write_backup(msg)
					self.write_backup(exception)
					try:
						import smtplib
						from email.message import EmailMessage
						import email.utils
			
						port = self.mailport
						# defaults to port 587
						if not port:
							# smtplib.SMTP_PORT defaults to port 25, which is intended for MTA-MTA comm
							# port 587 supports server -> client email comm 
							#port = smtplib.SMTP_PORT
							port = 587

						smtp = smtplib.SMTP(self.mailhost, port, timeout=self.timeout)
						msg = EmailMessage()
						msg['From'] = self.fromaddr
						msg['To'] = ','.join(self.toaddrs)
						msg['Subject'] = self.getSubject(exception)
						msg['Date'] = email.utils.localtime()
						msg.set_content(msg)
						
						if self.username:
							# TLS handshake
							if self.secure is not None:
								smtp.ehlo()
								# could potentially throw error if smtp server does not support STARTTLS
								# or a snake does a man in the middle attack 
								smtp.starttls(*self.secure)
								smtp.ehlo()
							smtp.login(self.username, self.password)
							smtp.send_message(msg)
							smtp.quit()
					except Exception:
						self.handleError(record)
			except Queue.Empty:
				break

	def write_backup(self, msg):
		self.backup_file.write(msg +"\n")

	def close(self):
		self.backup_file.close()
		self.producer.stop()
		logging.Handler.close(self)
