st.subheader("💬 LLM ChatBot")

        # ✅ 채팅 기록 저장
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []

        st.markdown("""
            <style>
            .chat-box {
                border: 2px solid #cccccc;
                border-radius: 12px;
                height: 400px;
                padding: 1rem;
                overflow-y: auto;
                background-color: transparent;
            }
            </style>
        """, unsafe_allow_html=True)

        # ✅ 채팅 박스 내부에서 메시지를 출력
        with st.container():
            # HTML로 빈 div 렌더
            st.markdown('<div class="chat-box">', unsafe_allow_html=True)

            # ✅ 메시지를 chat_message로 출력 (박스 안에서 실행되도록 보장)
            for message in st.session_state.chat_history:
                # 👇 이 구조가 실제 div 안에 들어가는 형태
                with st.chat_message("user"):
                    st.markdown(message["user"])
                with st.chat_message("assistant"):
                    st.markdown(message["assistant"])

            st.markdown('</div>', unsafe_allow_html=True)

        # ✅ 입력창은 밖에 고정
        if prompt := st.chat_input("질문을 입력하세요"):
            # 사용자 메시지 먼저 넣고, 서버에 응답 요청
            with st.chat_message("user"):
                st.markdown(prompt)

            # 응답 대기
            with st.chat_message("assistant"):
                with st.spinner("AI가 응답 중입니다..."):
                    try:
                        res = requests.post("http://localhost:5000/chat", json={"prompt": prompt})
                        if res.status_code == 200:
                            response = res.json().get("response", "⚠️ 빈 응답")
                        else:
                            response = f"❌ 오류 코드: {res.status_code}"
                    except Exception as e:
                        response = f"❌ 예외 발생: {e}"
                    st.markdown(response)

            # 채팅 기록에 저장
            st.session_state.chat_history.append({
                "user": prompt,
                "assistant": response
            })

        # ✅ 보고서 생성 버튼 (하단에 추가적으로 유지)
        st.button("📄 한글 보고서 생성")