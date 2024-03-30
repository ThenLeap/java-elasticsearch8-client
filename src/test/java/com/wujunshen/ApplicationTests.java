package com.wujunshen;

import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ActiveProfiles(value = "local")
@ExtendWith(SpringExtension.class)
@SpringBootTest
public class ApplicationTests {}
