import os
import sys

import news_deduper

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
# import config_reader
import cloud_amqp_client
import mongodb_client
from queue_cleaner import clear_queue

DB_NAME = 'my-tab-news'
COLLECTION_NAME = 'test'

QUEUE_URL = None
QUEUE_NAME = "test_dedupe_task_queue"

TEST_DEDUPE_TASK = [
    {'digest': 'some1', 'title': 'Uber pulls self-driving cars after first fatal crash of autonomous vehicle', 'publishedAt': '2018-03-18T20:36:47Z', 'text': 'Uber has removed its self-driving cars from the roads following what is believed to be the first fatality involving a fully autonomous car.\n\nA self-driving Uber SUV struck and killed 49-year-old Elaine Herzberg as she walked her bicycle across a street in Tempe, Arizona, Sunday night, according to the Tempe police. The department is investigating the crash.\n\nRafael Vasquez, a 44-year-old test driver from Uber, was behind the wheel of the Volvo XC90 SUV at the time, the police said.\n\nBased on preliminary information, the car was going approximately 40 mph in a 35 mph zone, according to Tempe Police Detective Lily Duran.\n\nPolice say the investigation does not at this time show significant signs of the SUV slowing before the crash. The Maricopa County Attorney\'s Office will determine whether charges will be filed.\n\n"The vehicle involved is one of Uber\'s self-driving vehicles," the Tempe police said in a statement. "It was in autonomous mode at the time of the collision, with a vehicle operator behind the wheel."\n\nAutonomous mode means the car is driving on its own. During tests, a person sits behind the wheel as a safeguard.\n\nUber is conducting tests of autonomous vehicles in Arizona, Pittsburgh, Toronto and other areas. Uber said it has stopped testing the vehicles throughout the United States and Canada.\n\nSome incredibly sad news out of Arizona. We\'re thinking of the victim\'s family as we work with local law enforcement to understand what happened. https://t.co/cwTCVJjEuz — dara khosrowshahi (@dkhos) March 19, 2018\n\nUber said it is "fully cooperating" with local officials. "Our hearts go out to the victim\'s family," Uber said in a statement.\n\nThe National Transportation Safety Board said it is launching an investigation.\n\nFor self-driving cars, dealing with pedestrians and bicyclists is a challenging task. The self-driving industry has found quicker success with highway driving, which is a less complicated environment.\n\nUber has previously grounded its vehicles while investigating a crash. In 2017, Uber briefly pulled its vehicles from roads after an Uber self-driving vehicle in Tempe landed on its side.\n\nArizona is a hotbed of self-driving car development. Earlier this month, Arizona Governor Doug Ducey updated an executive order to allow self-driving cars to drive on state roads without a test driver behind the wheel.\n\nRelated: Loophole would protect self-driving companies from lawsuits\n\nMeanwhile, Waymo, the self-driving arm of Google\'s parent company, is launching a public self-driving car service this year in the Phoenix, Arizona, area. Companies such as GM\'s Cruise and Intel are also testing in the state.\n\nArizona has little inclement weather. This makes it more appealing for self-driving cars, which can struggle in rain or during snowfall.\n\nThis isn\'t the first futuristic car involved in a fatal crash. In 2016, a man driving a Tesla was killed while its autopilot system was activated. But Tesla Autopilot is partially autonomous. A human driver is required to handle much of the driving.\n\nHowever, Uber\'s system is designed to fully replace a human driver.', 'source': 'cnn', 'url': 'http://money.cnn.com/2018/03/19/technology/uber-autonomous-car-fatal-crash/index.html'},
    {'digest': 'some2', 'publishedAt': '2018-03-18T20:36:47Z', 'text': 'People riding in self-driving cars that crash could find themselves unable to hold the manufacturer accountable if legislation currently pending on Capitol Hill is passed, consumer advocates warn.\n\nThey caution that the legislation under consideration in the Senate benefits big tech and car companies at the expense of average Americans.\n\nLegislators have pushed to clear the way for self-driving vehicles. The House unanimously passed a related bill in September. But critics warn that autonomous vehicles won\'t be perfect, and could have defects that lead to bad crashes. Like all computer systems, they will be vulnerable to hacking.\n\nAmong other potential problems, research has shown, for instance, that placing stickers on a stop sign can cause a self-driving car to misinterpret it as a speed limit sign, which could trigger a crash at an intersection.\n\nCritics are concerned that the bill, known as the AV Start Act, does not prohibit forced arbitration between autonomous vehicle manufacturers and consumers. A person badly injured while riding in a self-driving car would not be able to take part in a class action lawsuit, or sue the maker of the technology. Instead, disputes would be settled in arbitration. Experts say arbitration shifts the balance of power in the favor of big businesses, because they generally hire the arbitrator and will be its repeat customer.\n\nArbitration proceedings are also private, so use of them would mean the public is more likely to be kept in the dark about flaws in self-driving vehicles.\n\n"The nightmare scenario is that someone is hurt because of a defect and it\'s dealt with through a confidential arbitration proceeding that nobody knows about, and then more people are hurt because no one found out about it," Ed Walters, who teaches robotics law at Georgetown Law and Cornell Tech, told CNN. "Congress could stick up for the right to sue by prohibiting these kind of clauses, but so far they haven\'t."\n\nWednesday afternoon, five Democratic Senators released a letter calling for changes in the bill in which they outlined concerns including the bill potentially pre-empting state and local traffic laws. But they were silent on the topic of forced arbitration.\n\nRelated: How free self-driving car rides could change everything\n\n"No one is suggesting anyone is looking to intentionally put something dangerous on the road. But it\'s human nature that the less trouble we\'re going to get in, the less cautious we may choose to be," said Jason K. Levine, executive director of the Center for Auto Safety, a non-profit advocacy group.\n\n"Going back 50 years, I\'ve never seen a more brazen attempt to escape the rule of safety law, and the role of the courts to be accessible to their victims," longtime consumer advocate Ralph Nader told CNN. "With their unproven, secretive technology that\'s fully hackable, the autonomous vehicle industry wants to close the door on federal safety protection and close the door to the court room."\n\nRidesharing companies Uber and Lyft already include forced arbitration in their terms of service with passengers. Consumers will likely first experience self-driving technology in a similar format, by requesting a ride through a smartphone app.\n\n"It\'s another example of large companies being able to tilt the table their way in terms of service agreements. It\'s a power shift created in a contract that nobody reads. Everybody clicks \'I accept,\'" Walters said. "You have rights to sue unless you sign them away. And companies have gotten very good at making you sign them away."\n\nRelated: For self-driving cars, car washes are a nightmare\n\nThe largest players in the self-driving car industry, Waymo, GM\'s Cruise, Ford, Zoox, Toyota, Tesla, Uber and Lyft, all declined to comment for this story. The Self-Driving Coalition for Safer Streets, an industry lobbying group, also declined to comment.\n\n"Imagine if I put my daughter in the back of a self-driving Uber to get to soccer practice, and the vehicle can\'t see in the fog or rain, and there\'s a tragedy," Levine said. "My level of ability to hold the company accountable is entirely constricted by something buried in tons and tons of pages of terms of service, for downloading the app."', 'source': 'cnn', 'url': 'http://money.cnn.com/2018/03/14/technology/self-driving-car-senate-loophole/index.html', 'title': 'Loophole would protect self-driving car companies from lawsuits'},
    {'digest': 'some3', 'title': 'Uber pulls... (a different title) ', 'publishedAt': '2018-03-18T20:36:47Z', 'text': '(missing some content) following what is believed to be the first fatality involving a fully autonomous car.\n\nA self-driving Uber SUV struck and killed 49-year-old Elaine Herzberg as she walked her bicycle across a street in Tempe, Arizona, Sunday night, according to the Tempe police. The department is investigating the crash.\n\nRafael Vasquez, a 44-year-old test driver from Uber, was behind the wheel of the Volvo XC90 SUV at the time, the police said.\n\nBased on preliminary information, the car was going approximately 40 mph in a 35 mph zone, according to Tempe Police Detective Lily Duran.\n\nPolice say the investigation does not at this time show significant signs of the SUV slowing before the crash. The Maricopa County Attorney\'s Office will determine whether charges will be filed.\n\n"The vehicle involved is one of Uber\'s self-driving vehicles," the Tempe police said in a statement. "It was in autonomous mode at the time of the collision, with a vehicle operator behind the wheel."\n\nAutonomous mode means the car is driving on its own. During tests, a person sits behind the wheel as a safeguard.\n\nUber is conducting tests of autonomous vehicles in Arizona, Pittsburgh, Toronto and other areas. Uber said it has stopped testing the vehicles throughout the United States and Canada.\n\nSome incredibly sad news out of Arizona. We\'re thinking of the victim\'s family as we work with local law enforcement to understand what happened. https://t.co/cwTCVJjEuz — dara khosrowshahi (@dkhos) March 19, 2018\n\nUber said it is "fully cooperating" with local officials. "Our hearts go out to the victim\'s family," Uber said in a statement.\n\nThe National Transportation Safety Board said it is launching an investigation.\n\nFor self-driving cars, dealing with pedestrians and bicyclists is a challenging task. The self-driving industry has found quicker success with highway driving, which is a less complicated environment.\n\nUber has previously grounded its vehicles while investigating a crash. In 2017, Uber briefly pulled its vehicles from roads after an Uber self-driving vehicle in Tempe landed on its side.\n\nArizona is a hotbed of self-driving car development. Earlier this month, Arizona Governor Doug Ducey updated an executive order to allow self-driving cars to drive on state roads without a test driver behind the wheel.\n\nRelated: Loophole would protect self-driving companies from lawsuits\n\nMeanwhile, Waymo, the self-driving arm of Google\'s parent company, is launching a public self-driving car service this year in the Phoenix, Arizona, area. Companies such as GM\'s Cruise and Intel are also testing in the state.\n\nArizona has little inclement weather. This makes it more appealing for self-driving cars, which can struggle in rain or during snowfall.\n\nThis isn\'t the first futuristic car involved in a fatal crash. In 2016, a man driving a Tesla was killed while its autopilot system was activated. But Tesla Autopilot is partially autonomous. A human driver is required to handle much of the driving.\n\nHowever, Uber\'s system is designed to fully replace a human driver.', 'source': 'cnn', 'url': 'http://money.cnn.com/2018/03/19/technology/uber-autonomous-car-fatal-crash/index.html'}
]

def init():
    global QUEUE_URL
    # try:
    #     config = config_reader.get_config('../config/config.json')
    # except Exception as e:
    #     print(e)
    #     sys.exit(1)
    config = os.environ
    # print(config)
    QUEUE_URL = config["dedupe_task_queue_url"]

    # remove all documents in the db
    test_collection = mongodb_client.get_db(DB_NAME).get_collection(COLLECTION_NAME)
    result = test_collection.remove()
    print('depepuer test: db cleaning result', result)

def test_basic():
    clear_queue(QUEUE_URL, QUEUE_NAME)
    dedupe_queue_client = cloud_amqp_client.AMQPClient(QUEUE_URL, QUEUE_NAME)
    dedupe_queue_client.connect()

    print('news_deduper_test: adding task onto the dedupe queue...')
    for task in TEST_DEDUPE_TASK:
        dedupe_queue_client.send_message(task)

    # some setting for the deduper module, so mind that the global variables in the module
    # are not tested
    news_deduper.DEDUPE_QUEUE_URL = QUEUE_URL
    news_deduper.DEDUPE_QUEUE_NAME = QUEUE_NAME
    news_deduper.COLLECTION_NAME = COLLECTION_NAME
    news_deduper.dedupe_queue_client = dedupe_queue_client
    news_deduper.SLEEP_TIME_IN_SECONDS = 1

    news_deduper.run(len(TEST_DEDUPE_TASK))

    test_collection = mongodb_client.get_db(DB_NAME).get_collection(COLLECTION_NAME)
    # for news in test_collection.find():
    #     print(news)
    print('news deduper: number of documents(expecting 2)', test_collection.count())
    assert test_collection.count() == 2
    print('news deduper test passed')

    dedupe_queue_client.close()

if __name__ == '__main__':
    init()
    test_basic()
