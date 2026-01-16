TODO

- [ ] Endpoint com as datas das próximas corridas
- [ ] Websocket de position
- [ ] Passear com o cachorro


query {
  currentYearNextMeeting {
    meetingKey
    meetingName
    meetingOfficialName
    location
    countryName
    countryFlag
    circuitShortName
    circuitType
    circuitImage
    gmtOffset
    dateStart
    dateEnd
    year
  }
  currentYearMeetings{
    meetingKey
    meetingName
    meetingOfficialName
    location
    countryName
    countryFlag
    circuitShortName
    circuitType
    circuitImage
    gmtOffset
    dateStart
    dateEnd
    year
  }
}

