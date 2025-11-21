package com.antares.db.backend.parser;

import com.antares.db.common.Error;

public class Tokenizer {
    private byte[] stat;
    private int pos;
    private String currentToken;
    private boolean flushToken;
    private Exception err;

    // region public
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    public String peek() throws Exception {
        if (err != null) {
            throw err;
        }
        if (flushToken) {
            String token = null;
            try {
                token = next();
            } catch (Exception e) {
                err = e;
                throw err;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    public void pop() {
        flushToken = true;
    }

    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<<".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length - pos);
        return res;
    }

    // region private
    private String next() throws Exception {
        if (err != null) {
            throw err;
        }
        return nextMetaState();
    }

    /**
     * 从输入字符流中提取下一个元Token(符号、字符串、标识符/关键字)
     */
    private String nextMetaState() throws Exception {
        while (true) {
            Byte b = peekByte();
            if (b == null) {
                return "";
            }
            if (!isBlank(b)) {
                break;
            }
            popByte();
        }

        Byte b = peekByte();
        if (isSymbol(b)) {
            popByte();
            return new String(new byte[] { b });
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;;
            throw err;
        }
    }

    /**
     * 解析被引号("或')包裹的字符串
     */
    private String nextQuoteState() throws Exception {
        Byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }   
            sb.append(new String(new byte[] {b}));
            popByte();
        }
        return sb.toString();
    }

    /**
     * 解析一个普通标识符或关键字(循环读取直至遇到 非[字母、数字、下划线] 或 空白符)
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                // 消耗掉空白符，下次解析跳过
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[] {b}));
            popByte();
        }
    }

    // region utils
    private Byte peekByte() {
        if (pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    private void popByte() {
        pos++;
        if (pos > stat.length) {
            pos = stat.length;
        }
    }

    static boolean isBlank(byte b) {
        return b == ' ' || b == '\n' || b == '\t';
    }

    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')');
    }

    static boolean isAlphaBeta(byte b) {
        return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z');
    }

    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }
}
