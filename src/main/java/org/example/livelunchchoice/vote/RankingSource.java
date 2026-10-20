package org.example.livelunchchoice.vote;

import java.util.List;
import org.example.livelunchchoice.vote.dto.VoteResultResponse;

// 전송 계약은 공유하고, 실행 모드에 따라 결과 공급자만 선택한다.
public interface RankingSource {
    List<VoteResultResponse> rankings();
}
