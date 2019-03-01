section .text

extern gettid
extern sched_yield

;	This is the first use of assembly in this codebase, a valid question is WHY?
;	The spinlock we implement here is performance critical, and simply put GCC
;	emits awful code.  The original C code is left in fastlock.cpp for reference
;	and x-plat.  The code generated for the unlock case is reasonable and left in
;	C++.

ALIGN 16
global fastlock_lock
fastlock_lock:
	; RDI points to the struct:
	;	uint16_t active
	;	uint16_t avail
	;	int32_t m_pidOwner
	;	int32_t m_depth
	
	; First get our TID and put it in ecx
	push rdi                ; we need our struct pointer (also balance the stack for the call)
	call gettid             ; get our thread ID (TLS is nasty in ASM so don't bother inlining)
	mov esi, eax            ; back it up in esi
	mov rdi, [rsp]          ; get our pointer back

	cmp [rdi+4], esi        ; Is the TID we got back the owner of the lock?
	je .LLocked             ; Don't spin in that case

	xor eax, eax            ; eliminate partial register dependency
	inc eax                 ; we want to add one
	lock xadd [rdi+2], ax   ; do the xadd, ax contains the value before the addition
	; eax now contains the ticket
	xor ecx, ecx
ALIGN 16
.LLoop:
	cmp [rdi], ax           ; is our ticket up?
	je .LLocked             ; leave the loop
	pause
	add ecx, 1000h          ; Have we been waiting a long time? (oflow if we have)
	                        ;	1000h is set so we overflow on the 1024*1024'th iteration (like the C code)
	jnc .LLoop              ; If so, give up our timeslice to someone who's doing real work
	; Like the compiler, you're probably thinking: "Hey! I should take these pushs out of the loop"
	;	But the compiler doesn't know that we rarely hit this, and when we do we know the lock is
	;	taking a long time to be released anyways.  We optimize for the common case of short
	;	lock intervals.  That's why we're using a spinlock in the first place
	push rsi
	push rax
	mov rax, 24             ; sys_sched_yield
	syscall                 ; give up our timeslice we'll be here a while
	pop rax
	pop rsi
	mov rdi, [rsp]          ; our struct pointer is on the stack already
	xor ecx, ecx            ; Reset our loop counter
	jmp .LLoop              ; Get back in the game
ALIGN 16
.LLocked:
	mov [rdi+4], esi        ; lock->m_pidOwner = gettid()
	inc dword [rdi+8]       ; lock->m_depth++
	add rsp, 8              ; fix stack
	ret

ALIGN 16
global fastlock_trylock
fastlock_trylock:
	; RDI points to the struct:
	;	uint16_t active
	;	uint16_t avail
	;	int32_t m_pidOwner
	;	int32_t m_depth
	
	; First get our TID and put it in ecx
	push rdi                ; we need our struct pointer (also balance the stack for the call)
	call gettid             ; get our thread ID (TLS is nasty in ASM so don't bother inlining)
	mov esi, eax            ; back it up in esi
	pop rdi                 ; get our pointer back

	cmp [rdi+4], esi        ; Is the TID we got back the owner of the lock?
	je .LRecursive          ; Don't spin in that case

	mov eax, [rdi]          ; get both active and avail counters
	mov ecx, eax            ; duplicate in ecx
	ror ecx, 16             ; swap upper and lower 16-bits
	cmp eax, ecx            ; are the upper and lower 16-bits the same?
	jnz .LAlreadyLocked     ;	If not return failure

	; at this point we know eax+ecx have [avail][active] and they are both the same
	add ecx, 10000h         ; increment avail, ecx is now our wanted value
	lock cmpxchg [rdi], ecx ;	If rdi still contains the value in eax, put in ecx (inc avail)
	jnz .LAlreadyLocked     ; If Z is not set then someone locked it while we were preparing
	xor eax, eax
	inc eax                 ; return SUCCESS! (eax=1)
	mov [rdi+4], esi        ; lock->m_pidOwner = gettid()
	mov dword [rdi+8], eax  ; lock->m_depth = 1
	ret
ALIGN 16
.LRecursive:
	xor eax, eax
	inc eax                 ; return SUCCESS! (eax=1)
	inc dword [rdi+8]       ; lock->m_depth++
	ret
ALIGN 16
.LAlreadyLocked:
	xor eax, eax            ; return 0;
	ret
