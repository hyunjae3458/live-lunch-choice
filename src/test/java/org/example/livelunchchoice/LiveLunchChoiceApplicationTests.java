package org.example.livelunchchoice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.example.livelunchchoice.vote.messaging.VoteEventPublisher;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@MockitoBean(types = VoteEventPublisher.class)
class LiveLunchChoiceApplicationTests {

    @Test
    void contextLoads() {
    }

}
