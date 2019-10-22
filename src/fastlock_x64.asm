section .text

extern gettid
extern sched_yield
extern g_longwaits
extern registerwait
extern clearwait

;	This is the first use of assembly in this codebase, a valid question is WHY?
;	The spinlock we implement here is performance critical, and simply put GCC
;	emits awful code.  The original C code is left in fastlock.cpp for reference
;	and x-plat.

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
	pop rdi                 ; get our pointer back

	cmp [rdi+4], esi        ; Is the TID we got back the owner of the lock?
	je .LLocked             ; Don't spin in that case

	xor eax, eax            ; eliminate partial register dependency
	inc eax                 ; we want to add one
	lock xadd [rdi+2], ax   ; do the xadd, ax contains the value before the addition
	; ax now contains the ticket
	mov edx, [rdi]
	cmp dx, ax              ; is our ticket up?
	je .LLocked             ; no need to loop
	; Lock is contended, so inform the deadlock detector
	push rax
	push rdi
	push rsi
	call registerwait
	pop rsi
	pop rdi
	pop rax
	; OK Start the wait loop
	xor ecx, ecx
ALIGN 16
.LLoop:
	mov edx, [rdi]
	cmp dx, ax              ; is our ticket up?
	je .LExitLoop           ; leave the loop
	pause
	add ecx, 1000h          ; Have we been waiting a long time? (oflow if we have)
	                        ;	1000h is set so we overflow on the 1024*1024'th iteration (like the C code)
	jnc .LLoop              ; If so, give up our timeslice to someone who's doing real work
	; Like the compiler, you're probably thinking: "Hey! I should take these pushs out of the loop"
	;	But the compiler doesn't know that we rarely hit this, and when we do we know the lock is
	;	taking a long time to be released anyways.  We optimize for the common case of short
	;	lock intervals.  That's why we're using a spinlock in the first place
	; If we get here we're going to sleep in the kernel with a futex
	push rsi
	push rax
	; Setup the syscall args
                            ; rdi ARG1 futex (already in rdi)
	mov esi, (9 | 128)      ; rsi ARG2 FUTEX_WAIT_BITSET_PRIVATE
                            ; rdx ARG3 ticketT.u (already in edx)
	xor r10d, r10d          ; r10 ARG4 NULL
	mov r8, rdi	            ; r8 ARG5 dup rdi
	xor r9d, r9d
	bts r9d, eax            ; r9 ARG6 mask
	mov eax, 202            ; sys_futex
	; Do the syscall
	lock or [rdi+12], r9d   ; inform the unlocking thread we're waiting
	syscall	                ; wait for the futex
	not r9d	                ; convert our flag into a mask of bits not to touch
	lock and [rdi+12], r9d  ; clear the flag in the futex control mask
	; cleanup and continue
	mov rcx, g_longwaits
	inc qword [rcx]         ; increment our long wait counter
	pop rax
	pop rsi
	xor ecx, ecx            ; Reset our loop counter
	jmp .LLoop              ; Get back in the game
ALIGN 16
.LExitLoop:
	push rsi
	push rdi
	call clearwait
	pop rdi
	pop rsi
ALIGN 16
.LLocked:
	mov [rdi+4], esi        ; lock->m_pidOwner = gettid()
	inc dword [rdi+8]       ; lock->m_depth++
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

ALIGN 16
global fastlock_unlock
fastlock_unlock:
	; RDI points to the struct:
	;	uint16_t active
	;	uint16_t avail
	;	int32_t m_pidOwner
	;	int32_t m_depth
	push r11
	sub dword [rdi+8], 1         ; decrement m_depth, don't use dec because it partially writes the flag register and we don't know its state
	jnz .LDone                   ; if depth is non-zero this is a recursive unlock, and we still hold it
	mov dword [rdi+4], -1        ; pidOwner = -1 (we don't own it anymore)
	mov ecx, [rdi]               ; get current active (this one)
	inc ecx                      ; bump it to the next thread
	mov [rdi], cx                ; give up our ticket (note: lock is not required here because the spinlock itself guards this variable)
	; At this point the lock is removed, however we must wake up any pending futexs
	mov r9d, 1                   ; eax is the bitmask for 2 threads
	rol r9d, cl                  ; place the mask in the right spot for the next 2 threads
ALIGN 16
.LRetryWake:
	mov r11d, [rdi+12]           ; load the futex mask
	and r11d, r9d                ; are any threads waiting on a futex?
	jz .LDone                    ; if not we're done.
	; we have to wake the futexs
                                 ; rdi ARG1 futex (already in rdi)
	mov esi, (10 | 128)          ; rsi ARG2 FUTEX_WAKE_BITSET_PRIVATE
	mov edx, 0x7fffffff          ; rdx ARG3 INT_MAX (number of threads to wake)
	xor r10d, r10d               ; r10 ARG4 NULL
	mov r8, rdi                  ; r8 ARG5 dup rdi
                                 ; r9 ARG6 mask (already set above)
	mov eax, 202                 ; sys_futex
	syscall
	cmp eax, 1                   ; did we wake as many as we expected?
	jnz .LRetryWake
.LDone:
	pop r11
	ret
