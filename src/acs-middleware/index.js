const { EventHubConsumerClient, EventHubProducerClient, earliestEventPosition } = require('@azure/event-hubs')
const {
  iotConsumerGroup,
  iotHubConnectionString,
  iotHubName,
  eventHubSenderConnectionString,
  sendGridApiKey,
  sendGridFromEmail,
  twilioAccountSID,
  twilioAuthToken,
  twilioFromNumber
} = require('../config')
const pgFormat = require('pg-format')
const db = require('../helpers/db')
const sgMail = require('@sendgrid/mail')
const twilio = require('twilio')

const client = new twilio(twilioAccountSID, twilioAuthToken)

sgMail.setApiKey(sendGridApiKey)

// This is the connection string for EventHub. We use this to push processed data for data mining.
const senderClient = new EventHubProducerClient(
  eventHubSenderConnectionString
)

let json_machines
let tags

const sendThresholdNotifyEmail = function (to, from, subject, text, html) {
  const msg = {
    to,
    from,
    subject,
    text,
    html
  }

  console.log(`Sending email for threshold to ${to}`)

  sgMail
    .send(msg)
    .then((response) => {
      console.log('Email for threshold has been sent.')
    })
    .catch((error) => {
      console.error(error)
    })
}

const sendThresholdNotifySMS = function (to, from, body) {
  console.log(`Sending SMS for threshold to ${to}`)
  client.messages.create({
    to,
    from,
    body
  }).then((res) => {
    console.log('SMS for threshold has been sent.')
  }).catch((err) => {
    console.log('error ', err)
  })
}

const printError = function (err) {
  console.log(err.message)
}

function printLongText(longtext) {
  let offset = 0

  while (offset < longtext.length) {
    console.log(longtext.slice(offset, offset + 30))
    offset += 30
  }
}

function buildInsert(table) {
  if (table === 'device_data' || table === 'alarms')
    return `INSERT INTO ${table}(device_id, machine_id, tag_id, timestamp, values, timedata, serial_number) VALUES %L`
  else
    return `INSERT INTO ${table}(device_id, machine_id, tag_id, timestamp, values, serial_number) VALUES %L`
}

const printMessage = async function (message) {
  let deviceId = 0
  let res

  if (!message) {
    // message has an error
    console.log('Received incorrect message format')

    return
  } else {
    deviceId = message.systemProperties['iothub-connection-device-id']
  }
  let offset = 0

  function converter(buff, start, len) {
    const slicedBuff = buff.slice(start, start + len)
    let ret = 0

    offset += len

    try {
      if (len === 1) {
        ret = slicedBuff.readUInt8()
      } else if (len === 2) {
        ret = slicedBuff.readUInt16BE()
      } else if (len === 4) {
        ret = slicedBuff.readUInt32BE()
      }
    } catch (err) {
      console.log('teltonika-id:', deviceId, 'error:', err)
      console.log(buff)
    }

    return ret
  }

  function getTagValue(buff, start, len, type = 'int32') {
    const slicedBuff = buff.slice(start, start + len)
    const ret = 0

    offset += len

    try {
      if (type === 'bool') {
        return !!(slicedBuff.readUInt8())
      } else if (type === 'uint8') {
        return slicedBuff.readUint8()
      } else if (type === 'int16') {
        return slicedBuff.readInt16BE()
      } else if (type === 'uint16') {
        return slicedBuff.readUInt16BE()
      } if (type === 'float') {
        return slicedBuff.readFloatBE()
      } else if (type === 'uint32') {
        return slicedBuff.readUInt32BE()
      }
    } catch (error) {
      console.log('tetonika-id: ', deviceId, 'error: ', error)
      console.log(type, len, start)
      printLongText(buff)
    }

    return ret
  }

  if (!Buffer.isBuffer(message.body)) {
    if (message.body.cmd === 'status') {
      try {
        if (message.body.status === 'Ok') {
          if (message.body.plc.status === 'No connection with PLC' && message.body.tcu && message.body.tcu.link_state === 1) {
            res = await db.query('SELECT * FROM device_configurations WHERE teltonika_id = $1', [deviceId])

            if (res && res.rows.length > 0) {
              await db.query('UPDATE device_configurations SET plc_type = $1, plc_serial_number = $2, plc_status = $3, tcu_type = $4, tcu_serial_number = $5, tcu_status = $6, body = $7 WHERE teltonika_id = $8', [0, message.body.tcu.serial_num, message.body.tcu.link_state, message.body.tcu.type, message.body.tcu.serial_num, message.body.tcu.link_state, message.body, deviceId])

              console.log('device configuration updated')
            } else {
              await db.query('INSERT INTO device_configurations(teltonika_id, plc_type, plc_serial_number, plc_status, tcu_type, tcu_serial_number, tcu_status, body) VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *', [deviceId, 0, message.body.tcu.serial_num, message.body.tcu.link_state, message.body.tcu.type, message.body.tcu.serial_num, message.body.tcu.link_state, message.body])

              console.log('device configuration added')
            }
          } else {
            res = await db.query('SELECT * FROM device_configurations WHERE teltonika_id = $1', [deviceId])

            if (res && res.rows.length > 0) {
              await db.query('UPDATE device_configurations SET plc_type = $1, plc_serial_number = $2, plc_status = $3, tcu_type = $4, tcu_serial_number = $5, tcu_status = $6, body = $7 WHERE teltonika_id = $8', [message.body.plc.type, message.body.plc.serial_num, message.body.plc.link_state, message.body.tcu.type, message.body.tcu.serial_num, message.body.tcu.link_state, message.body, deviceId])

              console.log('device configuration updated')
            } else {
              await db.query('INSERT INTO device_configurations(teltonika_id, plc_type, plc_serial_number, plc_status, tcu_type, tcu_serial_number, tcu_status, body) VALUES($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *', [deviceId, message.body.plc.type, message.body.plc.serial_num, message.body.plc.link_state, message.body.tcu.type, message.body.tcu.serial_num, message.body.tcu.link_state, message.body])

              console.log('device configuration added')
            }
          }

          console.log('teltonika-id:', deviceId, message.body)
        }
      } catch (err) {
        console.log(err)
      }
    }

    return
  }

  const rowsToInsert = []
  const alarmsRowsToInsert = []
  const utilizationRowsToInsert = []
  const energyConsumptionRowsToInsert = []
  const runningRowsToInsert = []
  const deviceTypeRowsToInsert = []
  const softwareVersionRowsToInsert = []
  const softwareBuildRowsToInsert = []
  const snMonthRowsToInsert = []
  const snYearRowsToInsert = []
  const snUnitRowsToInsert = []
  const idleRowsToInsert = []

  const insertRows = [{
    property: 'capacity_utilization',
    table: 'utilizations',
    rows: utilizationRowsToInsert
  }, {
    property: 'energy_consumption',
    table: 'energy_consumptions',
    rows: energyConsumptionRowsToInsert
  }, {
    property: 'running',
    table: 'runnings',
    rows: runningRowsToInsert
  }, {
    property: 'idle',
    table: 'idle',
    rows: idleRowsToInsert
  }, {
    property: 'device_type',
    table: 'device_types',
    rows: deviceTypeRowsToInsert
  }, {
    property: 'software_version',
    table: 'software_version',
    rows: softwareVersionRowsToInsert
  }, {
    property: 'software_build',
    table: 'software_builds',
    rows: softwareBuildRowsToInsert
  }, {
    property: 'serial_number_month',
    table: 'serial_number_month',
    rows: snMonthRowsToInsert
  }, {
    property: 'serial_number_year',
    table: 'serial_number_year',
    rows: snYearRowsToInsert
  }, {
    property: 'serial_number_unit',
    table: 'serial_number_unit',
    rows: snUnitRowsToInsert
  }]

  const commandNumber = converter(message.body, 0, 1)

  console.log('command', commandNumber, 'deviceId', deviceId)

  if (commandNumber === 247) {
    const groupNum = converter(message.body, 1, 4)
    const sendingData = []

    for (let N = 0; N < groupNum; N++) {
      const group = {}

      group.timestamp = converter(message.body, offset, 4) // group timestamp
      const deviceType = converter(message.body, offset, 2) // device type - (03 f3) -> (1011)
      const deviceSerialNumber = converter(message.body, offset, 4) // device serial number

      const machine = json_machines.find((item) => item.device_type === deviceType)

      const machineId = machine ? machine.id : 11

      group.values = []

      const valCount = converter(message.body, offset, 4)  //9

      for (let M = 0; M < valCount; M++) {
        const val = {}

        val.id = converter(message.body, offset, 2) // tag id
        val.status = converter(message.body, offset, 1)  // status

        // Proceed only if status == 0x00
        if (val.status !== 0) {
          return
        }

        val.values = []
        const numOfElements = converter(message.body, offset, 1) // Array size
        const byteOfElement = converter(message.body, offset, 1) // Element size

        let plctag = false

        for (let i = 0; i < numOfElements; i++) {
          if (val.id === 32769) {
            val.values.push(getTagValue(message.body, offset, byteOfElement, 'bool'))
          } else {
            plctag = json_machines[machineId - 1].full_json.plctags.find((tag) => {
              return tag.id === val.id
            })

            if (plctag) {
              const { type } = plctag

              val.values.push(getTagValue(message.body, offset, byteOfElement, type))
            } else {
              printLongText(message.body)
              console.log('Can\'t find tag', val.id, 'machine-id:', machineId, 'teltonika-id:', deviceId)

              return
            }
          }
        }

        const date = new Date(group.timestamp * 1000)

        console.log('teltonika-id:', deviceId, 'Plc Serial Number', deviceSerialNumber, 'tag id:', val.id, 'timestamp:', date.toISOString(), 'configuration:', machineId, plctag.name, 'values:', JSON.stringify(val.values), 'machineID', machineId)

        const queryValuesWithTimeData = [deviceId, machineId, val.id, group.timestamp, JSON.stringify(val.values), date.toISOString(), deviceSerialNumber]  // queryValues for device_data and alarms
        const queryValuesWithoutTimeData = [deviceId, machineId, val.id, group.timestamp, JSON.stringify(val.values), deviceSerialNumber]  // queryValues for others

        let tagObj = null

        try { // eslint-disable-next-line
          tagObj = tags.find((tag) => parseInt(tag.configuration_id) === parseInt(machineId) && parseInt(tag.tag_id) === parseInt(val.id))
        } catch (error) {
          console.log('Qeury from tags table failed.')

          return
        }

        if (tagObj) {
          tagObj.timestamp = group.timestamp

          const insert = insertRows.find((insert) => insert.property === tagObj.tag_name)

          if (insert) insert.rows.push(queryValuesWithoutTimeData)
        }

        // check if the blender hopper is cleared
        if (Number(machineId) === 1 && val.id === 14 ) {
          try { //eslint-disable-next-line
            const res = await db.query('SELECT * FROM device_data WHERE serial_number = $1 AND tag_id = $2 ORDER BY timestamp DESC limit 1', [deviceSerialNumber, 14])
            let lastInv = 0

            // get total amount of last inventory and streaming inventory
            if (res.rows.length) {
              lastInv = arrSum(res.rows[0].values)
            }

            const currentInv = arrSum(val.values)

            // if the total amount of streaming inventory is bigger than last inventory, it means the hopper cleared
            if (currentInv < lastInv) { // eslint-disable-next-line
              const cleared = await db.query('SELECT * FROM hopper_cleared_time WHERE serial_number = $1 AND device_id = $2', [deviceSerialNumber, deviceId])

              if (cleared.rows.length === 0) {  // eslint-disable-next-line
                await db.query('INSERT INTO hopper_cleared_time(serial_number, timestamp, last_cleared_time, device_id, machine_id) VALUES ($1, $2, $3, $4, $5) RETURNING *', [deviceSerialNumber, group.timestamp, date, deviceId, machineId])
              } else { // eslint-disable-next-line
                await db.query('UPDATE hopper_cleared_time SET timestamp = $1, last_cleared_time = $2 WHERE serial_number = $3 AND device_id = $4', [group.timestamp, date, deviceSerialNumber, deviceId])
              }
              console.log('Hopper cleared time table has been updated.')
            }
          } catch (error) {
            console.log(error)
          }
        }

        // check if the tag is alarms
        try { // eslint-disable-next-line
          res = await db.query('SELECT * FROM alarm_types WHERE machine_id = $1 AND tag_id = $2', [machineId, val.id])

          if (res && res.rows.length > 0) {
            alarmsRowsToInsert.push(queryValuesWithTimeData)
            // check if the alarm is activated or deactivated
            for (let j = 0; j < res.rows.length; j ++) {
              try { // eslint-disable-next-line
                const alarmData = await db.query('SELECT * FROM alarm_status WHERE tag_id = $1 AND machine_id = $2 AND device_id = $3 AND "offset" = $4 ORDER BY timestamp DESC LIMIT 1', [val.id, machineId, deviceId, res.rows[j].offset])
                const streamingValue = parseInt(res.rows[j].bytes) ? (parseInt(val.values[0]) >> res.rows[j].offset) & res.rows[j].bytes : val.values[res.rows[j].offset]

                if (streamingValue) { // eslint-disable-next-line
                  const active_alarm = await db.query('SELECT * FROM active_alarms WHERE device_id = $1 AND tag_id = $2 AND "offset" = $3 AND machine_id = $4 AND bytes = $5 LIMIT 1', [deviceId, val.id, res.rows[j].offset, machineId, res.rows[j].bytes])

                  if (active_alarm.rows.length) { // eslint-disable-next-line
                    await db.query('UPDATE active_alarms SET timestamp = $1 WHERE device_id = $2 AND tag_id = $3 AND "offset" = $4 AND machine_id = $5 AND bytes = $6', [group.timestamp, deviceId, val.id, res.rows[j].offset, machineId, res.rows[j].bytes])
                  } else {  // eslint-disable-next-line
                    await db.query('INSERT INTO active_alarms(device_id, tag_id, "offset", timestamp, machine_id, serial_number, bytes) VALUES($1, $2, $3, $4, $5, $6, $7) RETURNING *', [deviceId, res.rows[j].tag_id, res.rows[j].offset, group.timestamp, res.rows[j].machine_id, deviceSerialNumber, res.rows[j].bytes])
                  }
                  console.log('Active alarms table has been updated.')
                } else { // eslint-disable-next-line
                  await db.query('DELETE FROM active_alarms WHERE machine_id = $2 AND tag_id = $3 AND "offset" = $4 AND device_id = $5 AND serial_number = $6', [res.rows[j].machine_id, res.rows[j].tag_id, res.rows[j].offset, deviceId, deviceSerialNumber])
                  console.log('Active alarms table has been updated.')
                }

                // if there is matching data with streaming data, compare that two values
                if (alarmData && alarmData.rows.length > 0) {
                  // calculate value of datas
                  const previousValue = alarmData.rows[0].is_activate

                  // compare the values and determine if streaming alarm is activate or deactivate
                  if (previousValue && !streamingValue) {
                    try { // eslint-disable-next-line
                      await db.query('INSERT INTO alarm_status(device_id, tag_id, "offset", timestamp, machine_id, is_activate) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *', [deviceId, parseInt(res.rows[j].tag_id), res.rows[j].offset, date.getTime(), machineId, false])
                      console.log('Alarm history has been updated')
                    } catch (error) {
                      console.log(error)
                    }
                  } else if (!previousValue && streamingValue) {
                    try { // eslint-disable-next-line
                      await db.query('INSERT INTO alarm_status(device_id, tag_id, "offset", timestamp, machine_id, is_activate) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *', [deviceId, parseInt(res.rows[j].tag_id), res.rows[j].offset, date.getTime(), machineId, true])
                      console.log('Alarm history has been updated')
                    } catch (error) {
                      console.log(error)
                    }
                  }
                }
                // if there is no matching data, save active alarms in the table
                else {
                  // check streaming value if the alarm is activate
                  if (streamingValue) {
                    try { // eslint-disable-next-line
                      await db.query('INSERT INTO alarm_status(device_id, tag_id, "offset", timestamp, machine_id, is_activate) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *', [deviceId, parseInt(res.rows[j].tag_id), parseInt(res.rows[j].offset), date.getTime(), machineId, true])
                      console.log('Alarm history has been updated')
                    } catch (error) {
                      console.log(error)
                    }
                  }
                }
              } catch (error) {
                console.log(error)
              }
            }
          }
        } catch (error) {
          console.log('Query from tags table failed.')

          return
        }

        rowsToInsert.push(queryValuesWithTimeData)

        sendingData.push({
          body: {
            'seerialNo': deviceSerialNumber,
            'tagId': val.id,
            'values': val.values
          }
        })

        try { // eslint-disable-next-line
          const conditions = await db.query('SELECT * FROM thresholds WHERE serial_number = $1 AND tag_id = $2', [deviceId, val.id])
          let value = 0

          // check for the default threshold conditions
          // eslint-disable-next-line no-await-in-loop
          await Promise.all(conditions.rows.map(async (condition) => {
            if (condition.bytes) {
              value = (val.values[0] >> condition.offset) & condition.bytes
            } else {
              value = val.values[condition.offset] / condition.multipled_by
            }

            if (condition.value) {
              if (compareThreshold(value, condition.operator, condition.value)) {
                console.log('Threshold option matched.')
                const estTime = new Date(date - 60 * 60 * 4 * 1000)

                try {
                  await db.query('UPDATE thresholds SET message_status = $1, last_triggered_at = $2, threshold_activated = $3 WHERE id = $4', [true, estTime.toISOString(), true, parseInt(condition.id)])

                  if (!condition.message_status) {
                    const user = await db.query('SELECT * FROM users WHERE id = $1 LIMIT 1', [condition.user_id])
                    const deviceName = await db.query('SELECT * FROM devices WHERE device_id = $1 LIMIT 1', [condition.device_id])
                    const thresholdTagName = await db.query('SELECT * FROM machine_tags WHERE tag_id = $1 AND "offset" = $2 LIMIT 1', [condition.tag_id, condition.offset])

                    if (thresholdTagName.rows.length && user.rows.length) {
                      const emailContent = `Hello ${user.rows[0].name}, a threshold alert has been triggered. ${deviceName.rows.length !== 0 ? deviceName.rows[0].name : '#'} has an alert of "${thresholdTagName.rows[0].name} ${condition.operator.toLowerCase()} ${condition.value}".`

                      if (condition.email_checked)
                        sendThresholdNotifyEmail(user.rows[0].email, sendGridFromEmail, 'Threshold alert has been triggered', emailContent, `<strong>${emailContent}</strong>`)
                      if (condition.sms_checked) {
                        const userProfile = await db.query('SELECT * FROM profiles WHERE user_id = $1 LIMIT 1', [condition.user_id])

                        if (userProfile.rows.length && userProfile.rows[0].phone) {
                          sendThresholdNotifySMS(parseToInternationalPhoneNumber(userProfile.rows[0].phone), twilioFromNumber, emailContent)
                        } else {
                          console.log('User profile does not exist or has no phone number.')
                        }
                      }
                    } else {
                      console.log('Incorrect option or User does not exist for the threshold.')
                    }
                  }
                  console.log('Threshold updated')
                } catch (error) {
                  console.log('Error occurred while updating thresholds table', error)
                }
              } else {
                try {
                  await db.query('UPDATE thresholds SET threshold_activated = $1 WHERE id = $2', [false, parseInt(condition.id)])
                } catch (error) {
                  console.log('Error occurred while updating thresholds table', error)
                }
              }
            }

            if (condition.approaching) {
              if (compareThreshold(value, condition.operator, condition.approaching)) {
                console.log('Threshold approaching option matched.')

                const estTime = new Date(date - 60 * 60 * 4 * 1000)

                try {
                  await db.query('UPDATE thresholds SET approaching_status = $1, approaching_triggered_time = $2, approaching_activated = $3 WHERE id = $4', [true, estTime.toISOString(), true, parseInt(condition.id)])

                  if (!condition.approaching_status) {
                    const user = await db.query('SELECT * FROM users WHERE id = $1 LIMIT 1', [condition.user_id])
                    const deviceName = await db.query('SELECT * FROM devices WHERE device_id = $1 LIMIT 1', [condition.device_id])
                    const thresholdTagName = await db.query('SELECT * FROM machine_tags WHERE tag_id = $1 AND "offset" = $2 LIMIT 1', [condition.tag_id, condition.offset])

                    if (thresholdTagName.rows.length && user.rows.length) {
                      const emailContent = `Hello ${user.rows[0].name}, a threshold alert has been triggered. ${deviceName.rows.length !== 0 ? deviceName.rows[0].name : '#'} has an alert of "${thresholdTagName.rows[0].name} ${condition.operator.toLowerCase()} ${condition.approaching}".`

                      if (condition.email_checked)
                        sendThresholdNotifyEmail(user.rows[0].email, sendGridFromEmail, 'Threshold alert has been triggered', emailContent, `<strong>${emailContent}</strong>`)
                      if (condition.sms_checked) {
                        const userProfile = await db.query('SELECT * FROM profiles WHERE user_id = $1 LIMIT 1', [condition.user_id])

                        if (userProfile.rows.length && userProfile.rows[0].phone) {
                          sendThresholdNotifySMS(parseToInternationalPhoneNumber(userProfile.rows[0].phone), twilioFromNumber, emailContent)
                        } else {
                          console.log('User profile does not exist or has no phone number.')
                        }
                      }
                    } else {
                      console.log('Incorrect option or User does not exist for the threshold.')
                    }
                  }
                  console.log('Threshold approaching status has been updated.')
                } catch (error) {
                  console.log('Error occured while updating thresholds table', error)
                }
              } else {
                try {
                  await db.query('UPDATE thresholds SET approaching_activated = $1 WHERE id = $2', [false, parseInt(condition.id)])
                } catch (error) {
                  console.log('Error occurred while updating thresholds table ', error)
                }
              }
            }

            if (condition.is_running) {
              if (compareThreshold(!!value, condition.operator, condition.is_running)) {
                console.log('Threshold alarm option matched.')

                const estTime = new Date(date - 60 * 60 * 4 * 1000)

                try {
                  await db.query('UPDATE thresholds SET message_status = $1, last_triggered_at = $2, threshold_activated = $3 WHERE id = $4', [true, estTime.toISOString(), true, parseInt(condition.id)])

                  if (!condition.message_status) {
                    const user = await db.query('SELECT * FROM users WHERE id = $1 LIMIT 1', [condition.user_id])
                    const deviceName = await db.query('SELECT * FROM devices WHERE device_id = $1 LIMIT 1', [condition.device_id])
                    const thresholdTagName = await db.query('SELECT * FROM alarm_types WHERE tag_id = $1 AND "offset" = $2 LIMIT 1', [condition.tag_id, condition.offset])

                    if (thresholdTagName.rows.length && user.rows.length) {
                      const emailContent = `Hello ${user.rows[0].name}, a threshold alert has been triggered. ${deviceName.rows.length !== 0 ? deviceName.rows[0].name : '#'} has an alert of "${thresholdTagName.rows[0].name} ${condition.operator.toLowerCase()} ${condition.is_running}".`

                      if (condition.email_checked)
                        sendThresholdNotifyEmail(user.rows[0].email, sendGridFromEmail, 'Threshold alert has been triggered', emailContent, `<strong>${emailContent}</strong>`)
                      if (condition.sms_checked) {
                        const userProfile = await db.query('SELECT * FROM profiles WHERE user_id = $1 LIMIT 1', [condition.user_id])

                        if (userProfile.rows.length && userProfile.rows[0].phone) {
                          sendThresholdNotifySMS(parseToInternationalPhoneNumber(userProfile.rows[0].phone), twilioFromNumber, emailContent)
                        } else {
                          console.log('User profile does not exist or has no phone number.')
                        }
                      }
                    } else {
                      console.log('Incorrect option or User does not exist for the threshold.')
                    }
                  }
                  console.log('Threshold updated')
                } catch (error) {
                  console.log(error)
                }
              } else {
                try {
                  await db.query('UPDATE thresholds SET approaching_activated = $1 WHERE id = $2', [false, parseInt(condition.id)])
                } catch (error) {
                  console.log('Error occurred while updating thresholds table ', error)
                }
              }
            }
          }))
        } catch (error) {
          console.log(error)
        }
      }
    }

    try {
      await senderClient.sendBatch(sendingData)
    } catch (error) {
      console.log(error, 'Sending failed.')
    }

    try {
      const promises = []

      promises.push(db.query(pgFormat(buildInsert('device_data'), rowsToInsert)))

      insertRows.forEach((insert) => {
        if (insert.rows.length)
          promises.push(db.query(pgFormat(buildInsert(insert.table), insert.rows)))
      })

      if (alarmsRowsToInsert.length) {
        promises.push(db.query(pgFormat(buildInsert('alarms'), alarmsRowsToInsert)))
      }

      await Promise.all(promises)
    } catch (error) {
      console.log('Inserting into database failed.')
      console.log(error)
    }
  }
}

function compareThreshold(actualValue, operator, targetValue) {
  switch (operator) {
  case 'Equals':
    return actualValue === Number(targetValue)
  case 'Does not equal':
    return actualValue !== Number(targetValue)
  case 'Is greater than':
    return actualValue > Number(targetValue)
  case 'Is greater than or equal to':
    return actualValue >= Number(targetValue)
  case 'Is less than':
    return actualValue < Number(targetValue)
  case 'Is less than or equal to':
    return actualValue <= Number(targetValue)
  default:
    return false
  }
}

const arrSum = (arr) => arr.reduce((a,b) => a + b, 0)

const parseToInternationalPhoneNumber = (phoneNumber) => {
  return `+1 ${phoneNumber.replace(/-/g, ' ')}`
}

async function getPlcConfigs() {
  try {
    const res = await db.query('SELECT * FROM machines ORDER BY id')

    return res.rows
  } catch (error) {
    console.log(error)

    return false
  }
}
async function getTags() {
  try {
    const res = await db.query('SELECT * FROM tags')

    return res.rows
  } catch (error) {
    console.log(error)

    return false
  }
}

module.exports = {
  start: async function() {
    json_machines = await getPlcConfigs()

    if (!json_machines) {
      console.log('Plc configs are not available.')
    } else {

      const db_batch_blender_plctags = []

      json_machines[0].full_json.plctags.forEach((plctag) => {
        db_batch_blender_plctags.push(plctag)
        if (plctag.id === 12) {
          plctag.dependents.forEach((dependent) => {
            db_batch_blender_plctags.push(dependent)
          })
        }
      })

      json_machines[0].full_json.plctags = db_batch_blender_plctags
    }

    tags = await getTags()

    //This is the connection string for IoThub. We use this to receive data from the devices using dedicated consumer client.
    const client = new EventHubConsumerClient(
      iotConsumerGroup,
      iotHubConnectionString,
      iotHubName
    )

    const partitionIds = await client.getPartitionIds()

    const subscriptionOptions = {
      startPosition: earliestEventPosition
    }

    partitionIds.map((id) => {
      return client.subscribe(
        id,
        {
          processEvents: async(events, context) => {
            // event processing code goes here
            printMessage(events[0])
          },
          processError: async(err, context) => {
            // error reporting/handling code here
            printError(err, context)
          }
        },
        subscriptionOptions
      )
    })
  }
}
